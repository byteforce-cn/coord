package cn.byteforce.coord.spring.annotation;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * 分布式缓存注解
 *
 * 标注在方法上，自动缓存返回值到 Coord Agent 内置缓存。
 * 与 Spring @Cacheable 可共存，顺序：Coord 缓存外层优先（本地内存，延迟 <1ms）。
 *
 * <pre>{@code
 * @CoordCacheable(key = "user:#{#userId}", ttl = 30, timeUnit = TimeUnit.SECONDS)
 * public User getUser(String userId) { ... }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CoordCacheable {

    /** 缓存 key（支持 SpEL 表达式） */
    String key();

    /** TTL 过期时间 */
    long ttl() default 60;

    /** 时间单位，默认 SECONDS */
    TimeUnit timeUnit() default TimeUnit.SECONDS;
}
