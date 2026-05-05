package cn.byteforce.mock.order.controller;

import cn.byteforce.mock.order.config.CoordAuthTokenHolder;
import cn.byteforce.mock.order.model.CreateOrderRequest;
import cn.byteforce.mock.order.model.Order;
import cn.byteforce.mock.order.service.CoordConfigService;
import cn.byteforce.mock.order.service.OrderService;
import coord.v1.Coord;
import coord.v1.RegistryServiceGrpc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.PostConstruct;
import java.util.Iterator;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class OrderController {

    @Autowired private OrderService orderService;
    @Autowired private CoordConfigService configService;
    @Autowired private RegistryServiceGrpc.RegistryServiceBlockingStub registryStub;
    @Autowired private CoordAuthTokenHolder authTokenHolder;

    @PostConstruct
    public void init() {
        orderService.ensureTransitKey();
    }

    /** 创建订单 */
    @PostMapping("/orders")
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest req) {
        try {
            Order order = orderService.createOrder(req);
            Map<String, Object> body = Map.of(
                    "orderId", order.getOrderId(),
                    "status", order.getStatus().name(),
                    "totalAmount", order.getTotalAmount(),
                    "workflowId", String.valueOf(order.getWorkflowId())
            );
            if (order.getStatus() == Order.Status.INVENTORY_INSUFFICIENT) {
                return ResponseEntity.status(409).body(body);
            }
            return ResponseEntity.ok(body);
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /** 查询订单 */
    @GetMapping("/orders/{orderId}")
    public ResponseEntity<?> getOrder(@PathVariable String orderId) {
        try {
            Order order = orderService.getOrder(orderId);
            return ResponseEntity.ok(Map.of(
                    "orderId", order.getOrderId(),
                    "userId", order.getUserId(),
                    "productId", order.getProductId(),
                    "quantity", order.getQuantity(),
                    "totalAmount", order.getTotalAmount(),
                    "status", order.getStatus().name(),
                    "encryptedPhone", String.valueOf(order.getEncryptedPhone()),
                    "workflowId", String.valueOf(order.getWorkflowId()),
                    "paymentId", String.valueOf(order.getPaymentId())
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** 查询订单（带存储的加密手机号，验证加密存储） */
    @GetMapping("/orders/{orderId}/details")
    public ResponseEntity<?> getOrderDetails(@PathVariable String orderId) {
        try {
            Order order = orderService.getOrder(orderId);
            return ResponseEntity.ok(Map.of(
                    "orderId", order.getOrderId(),
                    "status", order.getStatus().name(),
                    "phone", String.valueOf(order.getEncryptedPhone()),
                    "totalAmount", order.getTotalAmount(),
                    "workflowId", String.valueOf(order.getWorkflowId())
            ));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** 手动触发支付（E2E 测试辅助） */
    @PostMapping("/orders/{orderId}/pay")
    public ResponseEntity<?> pay(@PathVariable String orderId) {
        Order order = orderService.getOrder(orderId);
        boolean ok = orderService.processPay(orderId, order.getTotalAmount());
        if (ok) {
            orderService.updateStatus(orderId, Order.Status.PAID);
            return ResponseEntity.ok(Map.of("success", true));
        }
        return ResponseEntity.status(500).body(Map.of("success", false));
    }

    /** 更新订单状态（E2E 测试辅助） */
    @PutMapping("/orders/{orderId}/status")
    public ResponseEntity<?> updateStatus(@PathVariable String orderId,
                                           @RequestBody Map<String, String> body) {
        String statusStr = body.get("status");
        try {
            Order.Status status = Order.Status.valueOf(statusStr);
            orderService.updateStatus(orderId, status);
            return ResponseEntity.ok(Map.of("updated", true));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid status: " + statusStr));
        }
    }

    /** 查询当前配置（测试用） */
    @GetMapping("/config")
    public ResponseEntity<Map<String, String>> getConfig() {
        configService.refresh();
        return ResponseEntity.ok(configService.allConfig());
    }

    /** 健康检查 */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "order-service"));
    }

    /** 列出全部订单（测试辅助） */
    @GetMapping("/orders")
    public ResponseEntity<?> listOrders() {
        return ResponseEntity.ok(orderService.allOrders().values());
    }

    /** 通过注册中心发现服务（测试辅助） */
    @GetMapping("/discover/{serviceName}")
    public ResponseEntity<?> discoverService(@PathVariable String serviceName) {
        try {
            Iterator<Coord.ServiceInstance> it = registryStub.discover(
                    Coord.ServiceQuery.newBuilder().setServiceName(serviceName).build());
            if (it.hasNext()) {
                Coord.ServiceInstance inst = it.next();
                String url = "http://" + inst.getHost() + ":" + inst.getPort();
                return ResponseEntity.ok(Map.of("url", url, "host", inst.getHost(),
                        "port", inst.getPort()));
            }
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            return ResponseEntity.status(503).body(Map.of("error", e.getMessage()));
        }
    }

    /** 测试辅助：重置订单服务所有内存状态（仅用于 E2E 测试隔离）。 */
    @PostMapping("/internal/reset")
    public ResponseEntity<Map<String, String>> resetState() {
        orderService.clearAll();
        return ResponseEntity.ok(Map.of("status", "ok"));
    }

    @PostMapping("/internal/set-coord-token")
    public ResponseEntity<Map<String, String>> setCoordToken(@RequestBody Map<String, String> body) {
        String token = body.get("token");
        authTokenHolder.setToken(token);
        return ResponseEntity.ok(Map.of("status", "ok"));
    }
}
