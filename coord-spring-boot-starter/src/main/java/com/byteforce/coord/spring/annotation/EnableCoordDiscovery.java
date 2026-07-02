package cn.byteforce.coord.spring.annotation;

import java.lang.annotation.*;

/**
 * 启用 Coord 服务注册发现
 *
 * 标注在 Spring Boot 主类或 @Configuration 类上，
 * 应用启动时自动注册到 Coord 注册中心，并维持心跳。
 *
 * <pre>{@code
 * @SpringBootApplication
 * @EnableCoordDiscovery(serviceName = "order-service")
 * public class Application { ... }
 * }</pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EnableCoordDiscovery {

    /** 服务名称，默认取 spring.application.name */
    String serviceName() default "";

    /** 服务端口，默认取 server.port */
    int port() default 0;

    /** 心跳间隔（毫秒），默认 10000ms */
    long heartbeatIntervalMs() default 10000;
}
