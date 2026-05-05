package cn.byteforce.e2e.util;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestDataFactory {
    public static String randomProductId() {
        return "PROD-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    }

    public static String randomUserId() {
        return "user-" + UUID.randomUUID().toString().substring(0, 6);
    }

    public static String randomKey(String prefix) {
        return prefix + "." + UUID.randomUUID().toString().substring(0, 6);
    }

    public static String randomLockName() {
        return "lock-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public static String randomWorkflowDef(String ns, String name) {
        return "document:\n"
                + "  dsl: '1.0.0'\n"
                + "  namespace: " + ns + "\n"
                + "  name: " + name + "\n"
                + "  version: '1.0.0'\n"
                + "do:\n"
                + "  - setResult:\n"
                + "      set:\n"
                + "        result: ok\n";
    }
}
