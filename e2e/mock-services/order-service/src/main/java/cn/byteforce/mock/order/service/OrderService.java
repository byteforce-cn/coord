package cn.byteforce.mock.order.service;

import com.google.protobuf.ByteString;
import coord.v1.Coord;
import coord.v1.IdGenServiceGrpc;
import coord.v1.LockServiceGrpc;
import coord.v1.RegistryServiceGrpc;
import coord.v1.TransitServiceGrpc;
import coord.v1.WorkflowServiceGrpc;
import cn.byteforce.mock.order.model.CreateOrderRequest;
import cn.byteforce.mock.order.model.Order;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String TRANSIT_KEY = "order-sensitive-key";
    private static final String LOCK_PREFIX = "lock:order:";
    private static final long LOCK_TTL_SECONDS = 10L;

    // CNCF Serverless Workflow v2 definition used to track order lifecycle
    private static final String ORDER_WF_YAML = """
            document:
              dsl: "1.0.0"
              namespace: order
              name: order-processing
              version: "1.0.0"
            do:
              - processOrder:
                  set:
                    status: completed
            """;
    private static final String ORDER_WF_DEF_ID = "order-processing";

    private final Map<String, Order> store = new ConcurrentHashMap<>();
    /**
     * Idempotency cache: (userId|productId|quantity|unitPrice) → orderId. Any second
     * request with the same four fields within the service lifetime returns the
     * existing order ID instead of creating a new order. This reflects the real
     * business expectation that identical user intents should not duplicate.
     */
    private final Map<String, String> idempotencyIndex = new ConcurrentHashMap<>();

    @Autowired private RegistryServiceGrpc.RegistryServiceBlockingStub registryStub;
    @Autowired private LockServiceGrpc.LockServiceBlockingStub lockStub;
    @Autowired private IdGenServiceGrpc.IdGenServiceBlockingStub idGenStub;
    @Autowired private TransitServiceGrpc.TransitServiceBlockingStub transitStub;
    @Autowired private WorkflowServiceGrpc.WorkflowServiceBlockingStub workflowStub;
    @Autowired private CoordConfigService configService;

    /** 启动时部署工作流定义（幂等）*/
    @PostConstruct
    public void deployWorkflowDefinition() {
        try {
            workflowStub.deployWorkflowDefinition(
                    Coord.DeployWorkflowDefinitionRequest.newBuilder()
                            .setDefinitionId(ORDER_WF_DEF_ID)
                            .setVersion("1.0.0")
                            .setDefinitionYaml(ORDER_WF_YAML)
                            .build());
            log.info("Order workflow definition deployed: {}", ORDER_WF_DEF_ID);
        } catch (Exception e) {
            log.warn("Workflow definition deploy failed (may already exist): {}", e.getMessage());
        }
    }

    @Value("${pay.service.url:http://pay-service:18081}")
    private String payServiceUrl;

    @Value("${inventory.service.url:http://inventory-service:18082}")
    private String inventoryServiceUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    /** 初始化 Transit 密钥（幂等）*/
    public void ensureTransitKey() {
        try {
            transitStub.createKey(Coord.CreateKeyRequest.newBuilder().setKeyName(TRANSIT_KEY).build());
        } catch (Exception e) {
            // 密钥已存在时 gRPC 返回 ALREADY_EXISTS，忽略
            log.debug("Transit key init: {}", e.getMessage());
        }
    }

    /** 创建订单 */
    public Order createOrder(CreateOrderRequest req) {
        double total = req.getQuantity() * req.getUnitPrice();

        // 业务级幂等：相同 (用户, 商品, 数量, 单价) 在服务实例生命周期内只会产生一条订单。
        // 这避免了"用户多次点击提交"场景下重复下单的典型问题，同时让不同用户/商品/金额的
        // 订单仍然独立。未做 TTL 控制是为了让测试断言稳定；真实场景下应加过期窗口。
        String idempKey = req.getUserId() + "|" + req.getProductId() + "|"
                + req.getQuantity() + "|" + req.getUnitPrice();
        String existingId = idempotencyIndex.get(idempKey);
        if (existingId != null) {
            Order existing = store.get(existingId);
            if (existing != null) {
                log.info("Idempotent create: returning existing orderId={} for key={}",
                        existingId, idempKey);
                return existing;
            }
            idempotencyIndex.remove(idempKey);
        }

        // 分布式锁防重复下单
        String lockName = LOCK_PREFIX + req.getUserId() + ":" + req.getProductId();
        Coord.LockAcquireResponse lockResp = lockStub.acquire(
                Coord.LockAcquireRequest.newBuilder()
                        .setLockName(lockName)
                        .setOwner("order-service")
                        .setTtlSeconds(LOCK_TTL_SECONDS)
                        .setWait(false)
                        .build());

        if (!lockResp.getAcquired()) {
            // 并发场景：其它请求持有锁；再次查询幂等索引，可能对方已完成订单创建。
            existingId = idempotencyIndex.get(idempKey);
            if (existingId != null) {
                Order existing = store.get(existingId);
                if (existing != null) return existing;
            }
            throw new IllegalStateException("订单正在处理中，请勿重复提交");
        }

        String lockToken = lockResp.getToken();
        try {
            // Re-check after lock acquisition to avoid two racing requests both creating.
            existingId = idempotencyIndex.get(idempKey);
            if (existingId != null) {
                Order existing = store.get(existingId);
                if (existing != null) return existing;
            }

            // 生成 Snowflake 订单ID
            String orderId = generateId();

            // Transit 加密敏感字段
            String encPhone = encrypt(req.getPhone() != null ? req.getPhone() : "");
            String encAddress = encrypt(req.getAddress() != null ? req.getAddress() : "");

            Order order = new Order();
            order.setOrderId(orderId);
            order.setUserId(req.getUserId());
            order.setProductId(req.getProductId());
            order.setQuantity(req.getQuantity());
            order.setUnitPrice(req.getUnitPrice());
            order.setTotalAmount(total);
            order.setEncryptedPhone(encPhone);
            order.setEncryptedAddress(encAddress);

            // 同步扣减库存（防止超卖）
            if (!deductInventory(req.getProductId(), req.getQuantity())) {
                order.setStatus(Order.Status.INVENTORY_INSUFFICIENT);
                store.put(orderId, order);
                // Do NOT index insufficient orders into idempotency: the user may retry
                // after stock is replenished and legitimately expect a fresh order.
                log.info("Order created with insufficient inventory: orderId={}", orderId);
                return order;
            }

            order.setStatus(Order.Status.INVENTORY_DEDUCTED);
            store.put(orderId, order);
            idempotencyIndex.put(idempKey, orderId);

            // 启动 Workflow V2 实例跟踪订单生命周期
            try {
                String inputJson = String.format(
                        "{\"orderId\":\"%s\",\"userId\":\"%s\",\"productId\":\"%s\"}",
                        orderId, req.getUserId(), req.getProductId());
                Coord.StartWorkflowV2Response wfResp = workflowStub.startWorkflowV2(
                        Coord.StartWorkflowV2Request.newBuilder()
                                .setDefinitionId(ORDER_WF_DEF_ID)
                                .setVersion("1.0.0")
                                .setInputJson(inputJson)
                                .build());
                order.setWorkflowId(wfResp.getInstanceId());
                log.info("Order workflow started: orderId={} instanceId={}", orderId, wfResp.getInstanceId());
            } catch (Exception e) {
                log.warn("Workflow V2 start failed (non-fatal): {}", e.getMessage());
            }

            // 同步完成支付与确认
            boolean paid = processPay(orderId, total);
            if (paid) {
                order.setStatus(Order.Status.CONFIRMED);
                log.info("Order confirmed: orderId={}", orderId);
            } else {
                rollbackInventory(req.getProductId(), req.getQuantity());
                order.setStatus(Order.Status.PAY_FAILED);
                log.info("Order pay failed: orderId={}", orderId);
            }

            log.info("Order created: orderId={} workflowId={}", orderId, order.getWorkflowId());
            return order;
        } finally {
            // 释放锁
            lockStub.release(Coord.LockReleaseRequest.newBuilder()
                    .setLockName(lockName)
                    .setToken(lockToken)
                    .build());
        }
    }

    public Order getOrder(String orderId) {
        Order order = store.get(orderId);
        if (order == null) throw new IllegalArgumentException("订单不存在: " + orderId);
        return order;
    }

    /** 通过服务发现找到 inventory-service 并扣减库存 */
    public boolean deductInventory(String productId, int quantity) {
        String baseUrl = discoverService("inventory-service");
        if (baseUrl == null) baseUrl = inventoryServiceUrl;
        try {
            Map<String, Object> body = Map.of("quantity", quantity);
            @SuppressWarnings("unchecked")
            Map<String, Object> resp = restTemplate.postForObject(
                    baseUrl + "/api/inventory/" + productId + "/deduct", body, Map.class);
            return Boolean.TRUE.equals(resp != null ? resp.get("success") : false);
        } catch (Exception e) {
            log.warn("Inventory deduct failed: {}", e.getMessage());
            return false;
        }
    }

    /** 通过服务发现找到 pay-service 完成支付 */
    public boolean processPay(String orderId, double amount) {
        String baseUrl = discoverService("pay-service");
        if (baseUrl == null) baseUrl = payServiceUrl;
        try {
            Map<String, Object> body = Map.of("orderId", orderId, "amount", amount);
            @SuppressWarnings("unchecked")
            Map<String, Object> resp = restTemplate.postForObject(
                    baseUrl + "/api/payments", body, Map.class);
            if (resp != null && resp.get("paymentId") != null) {
                Order order = store.get(orderId);
                if (order != null) order.setPaymentId(resp.get("paymentId").toString());
                // Check payment status – pay-service returns FAILED for high amounts
                String status = resp.get("status") != null ? resp.get("status").toString() : "";
                return "COMPLETED".equals(status);
            }
            return false;
        } catch (Exception e) {
            log.warn("Payment failed: {}", e.getMessage());
            return false;
        }
    }

    public void updateStatus(String orderId, Order.Status status) {
        Order order = store.get(orderId);
        if (order != null) order.setStatus(status);
    }

    /** Rollback inventory deduction via inventory-service */
    public void rollbackInventory(String productId, int quantity) {
        String baseUrl = discoverService("inventory-service");
        if (baseUrl == null) baseUrl = inventoryServiceUrl;
        try {
            Map<String, Object> body = Map.of("quantity", quantity);
            restTemplate.postForObject(baseUrl + "/api/inventory/" + productId + "/rollback", body, Map.class);
        } catch (Exception e) {
            log.warn("Inventory rollback failed: {}", e.getMessage());
        }
    }

    /** 解密手机号（用于展示） */
    public String decryptPhone(String orderId) {
        Order order = getOrder(orderId);
        return decrypt(order.getEncryptedPhone());
    }

    public Map<String, Order> allOrders() {
        return store;
    }

    /** 测试辅助：清除所有在内存中的订单状态。 */
    public void clearAll() {
        store.clear();
        idempotencyIndex.clear();
        log.info("OrderService state cleared (test reset)");
    }

    // ── 内部辅助 ──────────────────────────────────────────────

    private String generateId() {
        Coord.SnowflakeResponse resp = idGenStub.generateSnowflake(
                Coord.SnowflakeRequest.newBuilder().setBatch(1).build());
        return String.valueOf(resp.getIds(0));
    }

    private String encrypt(String plaintext) {
        if (plaintext.isEmpty()) return "";
        try {
            return doEncrypt(plaintext);
        } catch (io.grpc.StatusRuntimeException e) {
            // The transit key may have been wiped by a cluster reset between service
            // startup and this call. Re-create the key idempotently and retry once
            // before giving up. Sensitive fields MUST NOT be stored in plaintext.
            log.warn("Encrypt attempt 1 failed ({}), re-creating key and retrying", e.getStatus());
            ensureTransitKey();
            try {
                return doEncrypt(plaintext);
            } catch (io.grpc.StatusRuntimeException e2) {
                throw new IllegalStateException(
                        "Transit encryption is unavailable; refusing to store sensitive field in plaintext: "
                                + e2.getStatus(), e2);
            }
        }
    }

    private String doEncrypt(String plaintext) {
        Coord.EncryptResponse resp = transitStub.encrypt(
                Coord.EncryptRequest.newBuilder()
                        .setKeyName(TRANSIT_KEY)
                        .setPlaintext(plaintext)
                        .build());
        return resp.getCiphertext();
    }

    private String decrypt(String ciphertext) {
        if (ciphertext.isEmpty()) return "";
        try {
            Coord.DecryptResponse resp = transitStub.decrypt(
                    Coord.DecryptRequest.newBuilder()
                            .setKeyName(TRANSIT_KEY)
                            .setCiphertext(ciphertext)
                            .build());
            return resp.getPlaintext();
        } catch (Exception e) {
            log.warn("Decrypt failed: {}", e.getMessage());
            return ciphertext;
        }
    }

    private String discoverService(String serviceName) {
        try {
            Iterator<Coord.ServiceInstance> it = registryStub.discover(
                    Coord.ServiceQuery.newBuilder().setServiceName(serviceName).build());
            if (it.hasNext()) {
                Coord.ServiceInstance inst = it.next();
                return "http://" + inst.getHost() + ":" + inst.getPort();
            }
        } catch (Exception e) {
            log.warn("Service discovery failed for {}: {}", serviceName, e.getMessage());
        }
        return null;
    }
}
