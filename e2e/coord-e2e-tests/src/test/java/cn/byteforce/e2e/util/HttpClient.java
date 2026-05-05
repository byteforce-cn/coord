package cn.byteforce.e2e.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HTTP client for Coord control plane and mock business services.
 */
@Component
public class HttpClient {
    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Value("${order.service.url:http://localhost:18080}") private String orderUrl;
    @Value("${pay.service.url:http://localhost:18081}") private String payUrl;
    @Value("${inventory.service.url:http://localhost:18082}") private String inventoryUrl;
    @Value("${coord.http.address:http://localhost:8080}") private String coordHttpUrl;

    /** Last order ID returned by createOrder (stateful for use in step chains). */
    private final AtomicReference<String> lastOrderIdRef = new AtomicReference<>();

    private WebClient client(String baseUrl) {
        return WebClient.builder().baseUrl(baseUrl).build();
    }

    // ── order-service ──────────────────────────────────────────────────────────

    /** @return HTTP status code */
    public int createOrder(Map<String, Object> body) {
        // Reset the cached order id so a failed request cannot be mistaken for a
        // successful one from a previous scenario running in the same JVM.
        lastOrderIdRef.set(null);
        try {
            String resp = client(orderUrl).post().uri("/api/orders")
                    .bodyValue(body).retrieve().bodyToMono(String.class)
                    .timeout(TIMEOUT).block();
            JsonNode node = parse(resp);
            if (node.has("orderId")) lastOrderIdRef.set(node.get("orderId").asText());
            else if (node.has("id")) lastOrderIdRef.set(node.get("id").asText());
            return 200;
        } catch (WebClientResponseException e) {
            // Parse response body even on error (e.g. 409 with orderId)
            try {
                JsonNode node = parse(e.getResponseBodyAsString());
                if (node.has("orderId")) lastOrderIdRef.set(node.get("orderId").asText());
            } catch (Exception jsonEx) {
                log.debug("Failed to parse error response body: {}", jsonEx.getMessage());
            }
            return e.getStatusCode().value();
        }
    }

    public String lastOrderId() {
        return lastOrderIdRef.get();
    }

    public String getOrderStatus(String orderId) {
        JsonNode n = getJson(orderUrl, "/api/orders/" + orderId);
        if (n.has("status")) return n.get("status").asText();
        return n.asText();
    }

    public JsonNode getOrderRaw(String orderId) {
        return getJson(orderUrl, "/api/orders/" + orderId);
    }

    public Map<String, Object> getOrderDetails(String orderId) {
        try {
            String resp = client(orderUrl).get()
                    .uri("/api/orders/" + orderId + "/details")
                    .retrieve().bodyToMono(String.class).timeout(TIMEOUT).block();
            return MAPPER.readValue(resp, new TypeReference<>() {});
        } catch (Exception e) {
            return Map.of();
        }
    }

    public int orderHealth() {
        return healthCheck(orderUrl, "/api/health");
    }

    public String getOrderConfig(String key) {
        JsonNode n = getJson(orderUrl, "/api/config");
        if (n.has(key)) return n.get(key).asText();
        return "";
    }

    public String discoverPayService() {
        JsonNode n = getJson(orderUrl, "/api/discover/pay-service");
        if (n.has("url")) return n.get("url").asText();
        return n.asText();
    }

    public String discoverInventoryService() {
        JsonNode n = getJson(orderUrl, "/api/discover/inventory-service");
        if (n.has("url")) return n.get("url").asText();
        return n.asText();
    }

    // ── pay-service ────────────────────────────────────────────────────────────

    public int payHealth() {
        return healthCheck(payUrl, "/api/health");
    }

    /** 动态设置 pay-service 支付失败率（0.0 正常，1.0 全部失败）。 */
    public void injectPayFault(double rate) {
        try {
            client(payUrl).post().uri("/api/payments/_inject-fault")
                    .bodyValue(Map.of("rate", rate))
                    .retrieve().bodyToMono(String.class).timeout(TIMEOUT).block();
        } catch (Exception e) {
            log.warn("injectPayFault({}) failed: {}", rate, e.getMessage());
        }
    }

    public String getPaymentForOrder(String orderId) {
        try {
            JsonNode n = getJson(payUrl, "/api/payments");
            if (n.isArray()) {
                for (JsonNode p : n) {
                    if (p.has("orderId") && orderId.equals(p.get("orderId").asText())) {
                        return p.get("paymentId").asText();
                    }
                }
            }
            if (n.has("paymentId")) return n.get("paymentId").asText();
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public String getPaymentStatus(String paymentId) {
        JsonNode n = getJson(payUrl, "/api/payments/" + paymentId);
        if (n.has("status")) return n.get("status").asText();
        return n.asText();
    }

    public JsonNode decryptCardToken(String paymentId) {
        return getJson(payUrl, "/api/payments/" + paymentId + "/decrypt-token");
    }

    // ── inventory-service ──────────────────────────────────────────────────────

    public int inventoryHealth(String productId) {
        return healthCheck(inventoryUrl, "/api/health");
    }

    public void inventoryInit(String productId, int quantity) {
        try {
            client(inventoryUrl).put().uri("/api/inventory/" + productId + "/init")
                    .bodyValue(Map.of("quantity", quantity))
                    .retrieve().bodyToMono(String.class).timeout(TIMEOUT).block();
        } catch (WebClientResponseException ignored) {}
    }

    public int getStock(String productId) {
        JsonNode n = getJson(inventoryUrl, "/api/inventory/" + productId);
        if (n.has("quantity")) return n.get("quantity").asInt();
        if (n.has("stock")) return n.get("stock").asInt();
        return n.asInt();
    }

    public JsonNode deductStock(String productId, int quantity) {
        return postJson(inventoryUrl, "/api/inventory/" + productId + "/deduct",
                Map.of("quantity", quantity));
    }

    public JsonNode rollbackStock(String productId, int quantity) {
        return postJson(inventoryUrl, "/api/inventory/" + productId + "/rollback",
                Map.of("quantity", quantity));
    }

    // ── concurrent helpers ─────────────────────────────────────────────────────

    public List<Integer> concurrentCreateOrders(int count, String productId, double unitPrice) {
        List<Mono<Integer>> monos = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final int uid = i;
            Map<String, Object> body = Map.of(
                    "userId", "user-concurrent-" + uid,
                    "productId", productId,
                    "quantity", 1,
                    "unitPrice", unitPrice);
            monos.add(client(orderUrl).post().uri("/api/orders")
                    .bodyValue(body).retrieve()
                    .onStatus(HttpStatusCode::isError, resp -> Mono.just(new WebClientResponseException(
                            resp.statusCode().value(), "error", null, null, null)))
                    .bodyToMono(String.class)
                    .map(r -> 200)
                    .onErrorReturn(WebClientResponseException.class, 409)
                    .timeout(TIMEOUT));
        }
        return Flux.merge(monos).collectList().block();
    }

    // ── coord HTTP ─────────────────────────────────────────────────────────────

    public String coordMetrics() {
        try {
            return client(coordHttpUrl).get().uri("/metrics")
                    .retrieve().bodyToMono(String.class).timeout(TIMEOUT).block();
        } catch (Exception e) {
            return "";
        }
    }

    // ── auth token propagation ────────────────────────────────────────────────

    /**
     * Reset stateful mock services to a clean slate before each test scenario.
     * Clears order/payment stores and resets fault injection rates.
     * Best-effort: failures are logged but do not abort the test.
     */
    public void resetMockServices() {
        for (String base : List.of(orderUrl, payUrl)) {
            try {
                client(base).post().uri("/api/internal/reset")
                        .retrieve().bodyToMono(String.class)
                        .timeout(TIMEOUT).block();
            } catch (Exception e) {
                log.debug("resetMockServices to {} failed (best-effort): {}", base, e.getMessage());
            }
        }
    }

    /**
     * Push the Coord auth token to all mock services so they can authenticate
     * their gRPC calls after the security domain is initialized.
     */
    public void pushAuthToken(String token) {
        Map<String, String> body = Map.of("token", token);
        for (String base : List.of(orderUrl, payUrl, inventoryUrl)) {
            try {
                client(base).post().uri("/api/internal/set-coord-token")
                        .bodyValue(body).retrieve().bodyToMono(String.class)
                        .timeout(TIMEOUT).block();
            } catch (Exception e) {
                log.debug("pushAuthToken to {} failed (best-effort): {}", base, e.getMessage());
            }
        }
    }

    // ── generic helpers ────────────────────────────────────────────────────────

    private JsonNode getJson(String base, String path) {
        try {
            String body = client(base).get().uri(path)
                    .retrieve().bodyToMono(String.class).timeout(TIMEOUT).block();
            return parse(body);
        } catch (WebClientResponseException e) {
            return MAPPER.getNodeFactory().numberNode(e.getStatusCode().value());
        }
    }

    private JsonNode postJson(String base, String path, Object body) {
        try {
            String resp = client(base).post().uri(path)
                    .bodyValue(body).retrieve().bodyToMono(String.class).timeout(TIMEOUT).block();
            return parse(resp);
        } catch (WebClientResponseException e) {
            return MAPPER.getNodeFactory().numberNode(e.getStatusCode().value());
        }
    }

    private int healthCheck(String base, String path) {
        try {
            client(base).get().uri(path).retrieve().bodyToMono(String.class).timeout(TIMEOUT).block();
            return 200;
        } catch (WebClientResponseException e) {
            return e.getStatusCode().value();
        } catch (Exception e) {
            return 503;
        }
    }

    private JsonNode parse(String body) {
        if (body == null) return MAPPER.nullNode();
        try { return MAPPER.readTree(body); }
        catch (Exception e) { return MAPPER.getNodeFactory().textNode(body); }
    }
}
