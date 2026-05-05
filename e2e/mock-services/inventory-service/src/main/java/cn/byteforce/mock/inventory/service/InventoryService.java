package cn.byteforce.mock.inventory.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class InventoryService {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    /** productId -> current stock */
    private final ConcurrentHashMap<String, Integer> stock = new ConcurrentHashMap<>();
    /** productId -> deducted (for rollback) */
    private final ConcurrentHashMap<String, Integer> deducted = new ConcurrentHashMap<>();

    /** 初始化库存（测试辅助） */
    public void initStock(String productId, int quantity) {
        stock.put(productId, quantity);
        log.info("Stock initialized: {}={}", productId, quantity);
    }

    public int getStock(String productId) {
        return stock.getOrDefault(productId, 0);
    }

    /** 分布式锁保证扣减原子性 */
    public boolean deduct(String productId, int quantity) {
        AtomicBoolean success = new AtomicBoolean(false);
        stock.compute(productId, (k, current) -> {
            if (current == null) current = 0;
            if (current >= quantity) {
                success.set(true);
                return current - quantity;
            }
            return current;
        });
        if (success.get()) {
            deducted.merge(productId, quantity, Integer::sum);
            log.info("Stock deducted: {} -{} remaining={}", productId, quantity, stock.getOrDefault(productId, 0));
        } else {
            log.warn("Insufficient stock: {} need {} have {}", productId, quantity, stock.getOrDefault(productId, 0));
        }
        return success.get();
    }

    /** 回滚库存 */
    public void rollback(String productId, int quantity) {
        stock.merge(productId, quantity, Integer::sum);
        log.info("Stock rolled back: {} +{}", productId, quantity);
    }

    public Map<String, Integer> allStock() { return stock; }
}
