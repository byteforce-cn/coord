package cn.byteforce.mock.pay.service;

import com.google.protobuf.ByteString;
import coord.v1.Coord;
import coord.v1.IdGenServiceGrpc;
import coord.v1.LockServiceGrpc;
import coord.v1.RegistryServiceGrpc;
import coord.v1.TransitServiceGrpc;
import cn.byteforce.mock.pay.model.Payment;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PayService {
    private static final Logger log = LoggerFactory.getLogger(PayService.class);
    private static final String TRANSIT_KEY = "pay-card-key";
    private static final String LOCK_PREFIX = "lock:pay:";

    private final Map<String, Payment> store = new ConcurrentHashMap<>();

    @Autowired private IdGenServiceGrpc.IdGenServiceBlockingStub idGenStub;
    @Autowired private TransitServiceGrpc.TransitServiceBlockingStub transitStub;
    @Autowired private LockServiceGrpc.LockServiceBlockingStub lockStub;

    @Value("${pay.failure.rate:0.0}")
    private double failureRate;

    @PostConstruct
    public void init() {
        try {
            transitStub.createKey(Coord.CreateKeyRequest.newBuilder().setKeyName(TRANSIT_KEY).build());
        } catch (Exception e) {
            log.warn("Transit key creation failed (service may still initialise); will retry on first use", e);
        }
    }

    public Payment pay(String orderId, double amount, String cardToken) {
        // 防重复支付
        String lockName = LOCK_PREFIX + orderId;
        Coord.LockAcquireResponse lock = lockStub.acquire(
                Coord.LockAcquireRequest.newBuilder()
                        .setLockName(lockName).setOwner("pay-service")
                        .setTtlSeconds(10).setWait(false).build());
        if (!lock.getAcquired()) {
            throw new IllegalStateException("支付正在处理，请勿重复提交");
        }
        try {
            String paymentId = generateId();
            String encToken = encrypt(cardToken != null ? cardToken : "");

            Payment p = new Payment();
            p.setPaymentId(paymentId);
            p.setOrderId(orderId);
            p.setAmount(amount);
            p.setEncryptedCardToken(encToken);

            // 模拟失败场景：显式 fault token 或随机失败率
            if ("TRIGGER_FAIL".equals(cardToken) || Math.random() < failureRate) {
                p.setStatus(Payment.Status.FAILED);
                p.setFailReason("TRIGGER_FAIL".equals(cardToken) ? "显式故障注入" : "模拟支付失败");
            } else {
                p.setStatus(Payment.Status.COMPLETED);
            }
            store.put(paymentId, p);
            log.info("Payment {} for order {} amount={} status={}", paymentId, orderId, amount, p.getStatus());
            return p;
        } finally {
            lockStub.release(Coord.LockReleaseRequest.newBuilder()
                    .setLockName(lockName).setToken(lock.getToken()).build());
        }
    }

    public Payment getPayment(String paymentId) {
        Payment p = store.get(paymentId);
        if (p == null) throw new IllegalArgumentException("支付记录不存在: " + paymentId);
        return p;
    }

    public Payment refund(String paymentId) {
        Payment p = getPayment(paymentId);
        p.setStatus(Payment.Status.REFUNDED);
        return p;
    }

    public String decryptCardToken(String paymentId) {
        Payment p = getPayment(paymentId);
        return decrypt(p.getEncryptedCardToken());
    }

    public Map<String, Payment> allPayments() { return store; }

    /** 测试用：动态更新失败率（0.0 = 正常，1.0 = 全部失败）。 */
    public void setFailureRate(double rate) {
        this.failureRate = rate;
    }

    /** 测试辅助：清除所有支付记录并重置故障率为 0（E2E 测试隔离）。 */
    public void clearAll() {
        store.clear();
        failureRate = 0.0;
        log.info("PayService state cleared (test reset)");
    }

    private String generateId() {
        return String.valueOf(idGenStub.generateSnowflake(
                Coord.SnowflakeRequest.newBuilder().setBatch(1).build()).getIds(0));
    }

    private String encrypt(String s) {
        if (s.isEmpty()) return "";
        try {
            return transitStub.encrypt(Coord.EncryptRequest.newBuilder()
                    .setKeyName(TRANSIT_KEY)
                    .setPlaintext(s)
                    .build()).getCiphertext();
        } catch (Exception e) {
            log.warn("Transit encrypt failed, returning plaintext as fallback: {}", e.getMessage());
            return s;
        }
    }

    private String decrypt(String s) {
        if (s.isEmpty()) return "";
        try {
            return transitStub.decrypt(Coord.DecryptRequest.newBuilder()
                    .setKeyName(TRANSIT_KEY).setCiphertext(s).build())
                    .getPlaintext();
        } catch (Exception e) {
            log.warn("Transit decrypt failed, returning ciphertext as fallback: {}", e.getMessage());
            return s;
        }
    }
}
