package cn.byteforce.e2e;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 
 * 最小化 Spring Boot 应用上下文入口，供 Cucumber Spring 集成使用。
 * 不运行任何 web server（仅作为测试 ApplicationContext 根）。
 */
@SpringBootApplication
public class E2ETestApp {
}
