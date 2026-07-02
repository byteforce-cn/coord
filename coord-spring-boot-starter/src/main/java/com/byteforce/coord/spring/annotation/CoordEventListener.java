package cn.byteforce.coord.spring.annotation;

import java.lang.annotation.*;

/**
 * Coord 事件监听器注解
 *
 * 标注在方法上，自动订阅 Coord 事件通知。
 * 事件格式遵循 CloudEvents 1.0 规范。
 *
 * <pre>{@code
 * @CoordEventListener(type = "order.created")
 * public void onOrderCreated(CloudEvent event) { ... }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CoordEventListener {

    /** 事件类型（CloudEvents type 字段） */
    String type();

    /** 事件来源（CloudEvents source 字段），为空表示不过滤 */
    String source() default "";
}
