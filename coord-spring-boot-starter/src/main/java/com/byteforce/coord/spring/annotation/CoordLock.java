package cn.byteforce.coord.spring.annotation;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * 分布式锁注解
 *
 * 标注在方法上，自动获取分布式锁，方法执行完毕后释放。
 *
 * 适用场景: 短持锁、低竞争的业务互斥
 * 不适用: 高竞争锁（建议用 Redis Redlock）、长持锁（>30s）
 *
 * <pre>{@code
 * @CoordLock(key = "order:#{#orderId}", timeout = 10000)
 * public void processOrder(String orderId) { ... }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CoordLock {

    /** 锁的 key（支持 SpEL 表达式） */
    String key();

    /** 获取锁超时时间（毫秒），默认 10000ms */
    long timeout() default 10000;

    /** 锁自动释放时间（毫秒），默认 30000ms */
    long leaseTimeMs() default 30000;

    /** 时间单位，默认 MILLISECONDS */
    TimeUnit timeUnit() default TimeUnit.MILLISECONDS;
}
