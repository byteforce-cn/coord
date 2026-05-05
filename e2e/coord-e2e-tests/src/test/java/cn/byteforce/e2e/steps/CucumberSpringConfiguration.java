package cn.byteforce.e2e.steps;

import cn.byteforce.e2e.E2ETestApp;
import cn.byteforce.e2e.SpringBootTestConfig;
import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@CucumberContextConfiguration
@SpringBootTest(classes = {E2ETestApp.class, SpringBootTestConfig.class})
@ActiveProfiles("test")
public class CucumberSpringConfiguration {
}
