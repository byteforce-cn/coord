package cn.byteforce.mock.pay.config;

import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class CoordAuthTokenHolder {
    private final AtomicReference<String> token = new AtomicReference<>();

    public String getToken() { return token.get(); }
    public void setToken(String t) { token.set(t); }
}
