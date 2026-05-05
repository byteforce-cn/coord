package cn.byteforce.mock.pay.controller;

import cn.byteforce.mock.pay.config.CoordAuthTokenHolder;
import cn.byteforce.mock.pay.model.Payment;
import cn.byteforce.mock.pay.service.PayService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class PayController {

    @Autowired private PayService payService;
    @Autowired private CoordAuthTokenHolder authTokenHolder;

    @PostMapping("/payments")
    public ResponseEntity<?> pay(@RequestBody Map<String, Object> body) {
        String orderId = String.valueOf(body.get("orderId"));
        double amount = Double.parseDouble(String.valueOf(body.get("amount")));
        String cardToken = body.containsKey("cardToken") ? String.valueOf(body.get("cardToken")) : null;
        try {
            Payment p = payService.pay(orderId, amount, cardToken);
            return ResponseEntity.ok(Map.of(
                    "paymentId", p.getPaymentId(),
                    "orderId", p.getOrderId(),
                    "amount", p.getAmount(),
                    "status", p.getStatus().name(),
                    "encryptedCardToken", String.valueOf(p.getEncryptedCardToken())
            ));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/payments/{id}")
    public ResponseEntity<?> getPayment(@PathVariable String id) {
        try {
            Payment p = payService.getPayment(id);
            return ResponseEntity.ok(Map.of(
                    "paymentId", p.getPaymentId(),
                    "orderId", p.getOrderId(),
                    "amount", p.getAmount(),
                    "status", p.getStatus().name()
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/payments/{id}/refund")
    public ResponseEntity<?> refund(@PathVariable String id) {
        Payment p = payService.refund(id);
        return ResponseEntity.ok(Map.of("status", p.getStatus().name()));
    }

    /** 解密卡号（仅测试用）*/
    @GetMapping("/payments/{id}/decrypt-token")
    public ResponseEntity<?> decryptToken(@PathVariable String id) {
        return ResponseEntity.ok(Map.of("cardToken", payService.decryptCardToken(id)));
    }

    @GetMapping("/payments")
    public ResponseEntity<?> list() {
        return ResponseEntity.ok(payService.allPayments().values());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "pay-service"));
    }

    /** 测试辅助：重置支付服务所有内存状态（仅用于 E2E 测试隔离）。 */
    @PostMapping("/internal/reset")
    public ResponseEntity<Map<String, String>> resetState() {
        payService.clearAll();
        return ResponseEntity.ok(Map.of("status", "ok"));
    }

    @PostMapping("/internal/set-coord-token")
    public ResponseEntity<Map<String, String>> setCoordToken(@RequestBody Map<String, String> body) {
        String token = body.get("token");
        authTokenHolder.setToken(token);
        return ResponseEntity.ok(Map.of("status", "ok"));
    }

    /**
     * 测试管理端点：动态注入支付失败率。
     * body: {"rate": 0.0~1.0}  或  {"rate": "always"} 表示 1.0。
     * 用 rate=0.0 恢复正常，用 rate=1.0 让所有支付失败。
     * 不影响 cardToken="TRIGGER_FAIL" 的显式失败路径。
     */
    @PostMapping("/payments/_inject-fault")
    public ResponseEntity<Map<String, String>> injectFault(@RequestBody Map<String, Object> body) {
        Object rateObj = body.get("rate");
        double rate;
        if ("always".equals(rateObj)) {
            rate = 1.0;
        } else {
            rate = Double.parseDouble(String.valueOf(rateObj));
        }
        payService.setFailureRate(rate);
        return ResponseEntity.ok(Map.of("status", "ok", "failureRate", String.valueOf(rate)));
    }
}
