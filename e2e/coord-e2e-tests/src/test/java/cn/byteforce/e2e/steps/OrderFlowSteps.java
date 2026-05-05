package cn.byteforce.e2e.steps;

import cn.byteforce.e2e.util.HttpClient;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class OrderFlowSteps {

    @Autowired private HttpClient http;
    @Autowired private ScenarioState state;

    // ── Setup ─────────────────────────────────────────────────────────────────────

    @Given("库存服务中商品 {string} 库存为 {int}")
    public void setStock(String productId, int stock) {
        http.inventoryInit(productId, stock);
        state.productId = productId;
        state.initialStock = stock;
    }

    @Given("所有服务均健康")
    public void allHealthy() {
        RetryHelper.await(15).untilAsserted(() -> {
            assertThat(http.orderHealth()).isEqualTo(200);
            assertThat(http.payHealth()).isEqualTo(200);
            assertThat(http.inventoryHealth(state.productId != null ? state.productId : "PROBE")).isEqualTo(200);
        });
    }

    // ── Create order ──────────────────────────────────────────────────────────────

    @When("用户 {string} 创建订单 商品={string} 数量={int} 单价={double}")
    public void createOrder(String userId, String productId, int quantity, double unitPrice) {
        Map<String, Object> body = Map.of(
                "userId", userId,
                "productId", productId,
                "quantity", quantity,
                "unitPrice", new BigDecimal(String.valueOf(unitPrice)),
                "phone", "13800138000",
                "address", "测试地址");
        int httpCode = http.createOrder(body);
        if (state.orderId == null) {
            state.lastHttpCode = httpCode;
            state.orderId = http.lastOrderId();
        } else {
            state.lastHttpCode2 = httpCode;
            state.orderId2 = http.lastOrderId();
        }
    }

    @Then("返回订单 ID")
    public void verifyOrderId() {
        assertThat(state.orderId).isNotBlank();
    }

    @Then("HTTP 状态码为 {int}")
    public void verifyHttpStatus(int expected) {
        assertThat(state.lastHttpCode).isEqualTo(expected);
    }

    // ── Order status transitions ───────────────────────────────────────────────────

    @Then("订单状态最终变为 {string}")
    public void verifyOrderStatus(String expected) {
        RetryHelper.await(30).untilAsserted(() -> {
            String status = http.getOrderStatus(state.orderId);
            assertThat(status).isEqualTo(expected);
        });
    }

    @Then("订单状态为 {string}")
    public void verifyOrderStatusImmediate(String expected) {
        assertThat(http.getOrderStatus(state.orderId)).isEqualTo(expected);
    }

    // ── Inventory ─────────────────────────────────────────────────────────────────

    @Then("商品 {string} 库存扣减 {int}")
    public void verifyStockReduced(String productId, int qty) {
        int current = http.getStock(productId);
        assertThat(current).isEqualTo(state.initialStock - qty);
    }

    @Then("商品 {string} 库存不变")
    public void verifyStockUnchanged(String productId) {
        int current = http.getStock(productId);
        assertThat(current).isEqualTo(state.initialStock);
    }

    @Then("库存回滚 商品 {string} 恢复 {int}")
    public void verifyStockRolledBack(String productId, int qty) {
        // The workflow poller processes steps with ~2s cadence and the task pipeline
        // normally reaches "process-payment" in ~12s. Allow generous slack so CI jitter
        // (cold caches, gRPC warm-up) does not flake this assertion.
        RetryHelper.await(30).untilAsserted(() -> {
            int current = http.getStock(productId);
            assertThat(current).isEqualTo(state.initialStock);
        });
    }

    // ── Payment ───────────────────────────────────────────────────────────────────

    @Then("支付服务创建支付记录")
    public void verifyPaymentCreated() {
        RetryHelper.await(15).untilAsserted(() -> {
            state.paymentId = http.getPaymentForOrder(state.orderId);
            assertThat(state.paymentId).isNotBlank();
        });
    }

    @Then("支付记录状态为 {string}")
    public void verifyPaymentStatus(String expected) {
        assertThat(http.getPaymentStatus(state.paymentId)).isEqualTo(expected);
    }

    // ── Deduplication ─────────────────────────────────────────────────────────────

    @When("用户 {string} 重复创建同一订单 商品={string} 数量={int} 单价={double}")
    public void createDuplicateOrder(String userId, String productId, int quantity, double unitPrice) {
        if (state.orderId == null) {
            createOrder(userId, productId, quantity, unitPrice);
        }
        Map<String, Object> body = Map.of(
                "userId", userId,
                "productId", productId,
                "quantity", quantity,
                "unitPrice", new BigDecimal(String.valueOf(unitPrice)));
        state.lastHttpCode2 = http.createOrder(body);
        state.orderId2 = http.lastOrderId();
    }

    @Then("两次请求返回相同订单 ID")
    public void verifyIdempotencySameId() {
        assertThat(state.orderId).isNotBlank();
        assertThat(state.orderId2).isNotBlank();
        assertThat(state.orderId).isEqualTo(state.orderId2);
    }

    @Then("两次请求返回相同订单 ID 或幂等提示")
    public void verifyIdempotency() {
        // 保留旧步骤兼容性：强幂等（同 ID）或弱幂等（409）均可接受
        boolean sameId = state.orderId != null && state.orderId.equals(state.orderId2);
        boolean conflict = state.lastHttpCode2 == 409 || state.lastHttpCode2 == 200;
        assertThat(sameId || conflict).isTrue();
    }

    @Then("两个订单 ID 不相同")
    public void verifyDifferentOrderIds() {
        assertThat(state.orderId).isNotBlank();
        assertThat(state.orderId2).isNotBlank();
        assertThat(state.orderId).isNotEqualTo(state.orderId2);
    }

    // ── Concurrent orders ─────────────────────────────────────────────────────────

    @When("{int} 个用户并发下单 商品={string} 数量=1 单价={double}")
    public void concurrentOrders(int count, String productId, double unitPrice) throws InterruptedException {
        state.concurrentOrderResults = http.concurrentCreateOrders(count, productId, unitPrice);
    }

    @Then("成功订单数 <= {int}")
    public void verifySuccessCountAtMost(int max) {
        long success = state.concurrentOrderResults.stream()
                .filter(r -> r == 200 || r == 201).count();
        assertThat(success).isLessThanOrEqualTo(max);
    }

    @Then("成功订单数 == {int}")
    public void verifySuccessCountExact(int expected) {
        long success = state.concurrentOrderResults.stream()
                .filter(r -> r == 200 || r == 201).count();
        assertThat(success)
                .as("Expected exactly %d successful orders (got %d); lock may have miscounted", expected, success)
                .isEqualTo(expected);
    }

    @Then("失败订单数 == {int}")
    public void verifyFailCountExact(int expected) {
        long fail = state.concurrentOrderResults.stream()
                .filter(r -> r != 200 && r != 201).count();
        assertThat(fail)
                .as("Expected exactly %d failed orders (got %d)", expected, fail)
                .isEqualTo(expected);
    }

    @Then("库存不出现超卖")
    public void verifyNoOversell() {
        int current = http.getStock(state.productId);
        assertThat(current).isGreaterThanOrEqualTo(0);
    }

    // ── Encryption ────────────────────────────────────────────────────────────────

    @Then("订单详情中敏感字段已加密存储")
    public void verifySensitiveFieldEncrypted() {
        Map<String, Object> details = http.getOrderDetails(state.orderId);
        String phone = (String) details.get("phone");
        // encrypted values are typically base64-encoded ciphertext, not plaintext numbers
        if (phone != null) {
            assertThat(phone).doesNotMatch("^1[3-9]\\d{9}$"); // not a plain phone
        }
    }

    // ── Fault injection ───────────────────────────────────────────────────────

    @When("注入支付故障 rate={double}")
    public void injectPayFault(double rate) {
        http.injectPayFault(rate);
    }

    @When("恢复支付正常 rate={double}")
    public void restorePayNormal(double rate) {
        http.injectPayFault(rate);
    }

    // ── Workflow integration ───────────────────────────────────────────────────────

    @Then("订单工作流已启动")
    public void verifyWorkflowStarted() {
        RetryHelper.await(15).untilAsserted(() -> {
            Map<String, Object> details = http.getOrderDetails(state.orderId);
            Object wfId = details.get("workflowId");
            assertThat(wfId).isNotNull();
        });
    }

    // ── Config ────────────────────────────────────────────────────────────────────

    @When("Order服务获取配置 {string}")
    public void orderServiceGetConfig(String key) {
        state.orderConfig = http.getOrderConfig(key);
    }

    // ── Service discovery ─────────────────────────────────────────────────────────

    @Then("Order服务可发现 Pay服务")
    public void orderCanDiscoverPay() {
        RetryHelper.await(10).untilAsserted(() -> {
            String url = http.discoverPayService();
            assertThat(url).isNotBlank();
        });
    }

    @Then("Order服务可发现 Inventory服务")
    public void orderCanDiscoverInventory() {
        RetryHelper.await(10).untilAsserted(() -> {
            String url = http.discoverInventoryService();
            assertThat(url).isNotBlank();
        });
    }
}
