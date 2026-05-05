package cn.byteforce.e2e;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.*;

@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = PLUGIN_PROPERTY_NAME,
        value = "pretty, html:target/cucumber-reports/report.html, json:target/cucumber-reports/report.json")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "cn.byteforce.e2e.steps")
@ConfigurationParameter(key = OBJECT_FACTORY_PROPERTY_NAME,
        value = "io.cucumber.spring.SpringFactory")
public class CucumberTestRunner {
}
