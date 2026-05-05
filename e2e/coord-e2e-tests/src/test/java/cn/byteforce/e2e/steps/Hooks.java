package cn.byteforce.e2e.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import coord.v1.Coord;
import coord.v1.SealServiceGrpc;
import cn.byteforce.e2e.util.HttpClient;
import io.cucumber.java.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class Hooks {
    private static final Logger log = LoggerFactory.getLogger(Hooks.class);

    /**
     * File-based cache for root token and unseal shares so that tests can be
     * re-run against an already-initialised cluster without a fresh start.
     */
    private static final Path CACHE_FILE = Paths.get(".cache", "e2e-security.json");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Once-per-JVM bootstrap guard. Populated the first time a scenario runs after the
     * security domain has been initialised; re-used for every subsequent scenario so
     * that features without explicit Seal/Unseal steps still carry a valid token.
     */
    private static volatile String cachedRootToken;
    private static volatile List<String> cachedUnsealShares = new ArrayList<>();
    private static final Object LOCK = new Object();

    /**
     * Called by SecuritySteps after operations that replace the unseal shares
     * (e.g. RotateRootKey) so that subsequent scenarios reset to the valid shares.
     */
    public static void updateCachedUnsealShares(List<String> newShares) {
        cachedUnsealShares = new ArrayList<>(newShares);
        persistCache(cachedRootToken, cachedUnsealShares);
    }

    @Autowired private ScenarioState state;
    @Autowired private AtomicReference<String> coordAuthToken;
    @Autowired private HttpClient httpClient;
    @Autowired private SealServiceGrpc.SealServiceBlockingStub sealStub;

    @Before(order = 0)
    public void bootstrapSecurityIfNeeded() {
        // If another scenario in this JVM already performed init, reuse the token.
        if (cachedRootToken != null) {
            propagateToken(cachedRootToken, cachedUnsealShares);
            return;
        }
        synchronized (LOCK) {
            if (cachedRootToken != null) {
                propagateToken(cachedRootToken, cachedUnsealShares);
                return;
            }
            try {
                Coord.GetSealStatusResponse status = sealStub.getSealStatus(
                        Coord.GetSealStatusRequest.newBuilder().build());
                if (!status.getInitialized()) {
                    Coord.InitSecurityResponse init = sealStub.initSeal(
                            Coord.InitSecurityRequest.newBuilder()
                                    .setSecretShares(5).setSecretThreshold(3).build());
                    cachedRootToken = init.getRootToken();
                    cachedUnsealShares = new ArrayList<>(init.getKeySharesList());
                    // Unseal immediately with threshold shares
                    for (int i = 0; i < 3; i++) {
                        sealStub.unseal(Coord.UnsealRequest.newBuilder()
                                .setKeyShare(cachedUnsealShares.get(i)).build());
                    }
                    persistCache(cachedRootToken, cachedUnsealShares);
                    log.info("Bootstrapped security domain: shares={}, threshold=3, token-cached",
                            cachedUnsealShares.size());
                } else {
                    // Cluster already initialised — try to recover from file cache.
                    if (loadCache()) {
                        log.info("Recovered root token from file cache (.cache/e2e-security.json)");
                    } else {
                        String hint = "Coord security domain is already initialised but no cached " +
                                "root token is available (neither in JVM nor in .cache/e2e-security.json). " +
                                "Run `make e2e-reset && make e2e-up` to start a fresh cluster, " +
                                "then rerun the tests.";
                        log.error(hint);
                        throw new IllegalStateException(hint);
                    }
                }
            } catch (io.grpc.StatusRuntimeException e) {
                log.error("Failed to bootstrap security domain: {}", e.getStatus(), e);
                throw e;
            }
            propagateToken(cachedRootToken, cachedUnsealShares);
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean loadCache() {
        try {
            if (!Files.exists(CACHE_FILE)) return false;
            Map<String, Object> data = MAPPER.readValue(CACHE_FILE.toFile(), Map.class);
            String token = (String) data.get("rootToken");
            List<String> shares = (List<String>) data.get("unsealShares");
            if (token == null || shares == null || shares.isEmpty()) return false;
            cachedRootToken = token;
            cachedUnsealShares = new ArrayList<>(shares);
            return true;
        } catch (IOException e) {
            log.warn("Failed to load security cache: {}", e.getMessage());
            return false;
        }
    }

    private static void persistCache(String token, List<String> shares) {
        try {
            Files.createDirectories(CACHE_FILE.getParent());
            MAPPER.writeValue(CACHE_FILE.toFile(),
                    Map.of("rootToken", token, "unsealShares", shares));
            log.debug("Persisted security cache to {}", CACHE_FILE);
        } catch (IOException e) {
            log.warn("Failed to persist security cache: {}", e.getMessage());
        }
    }

    private void propagateToken(String token, List<String> shares) {
        coordAuthToken.set(token);
        state.sealRootToken = token;
        state.unsealShares = new ArrayList<>(shares);

        // If the cluster was left sealed by a prior scenario (e.g. "密封安全域" without
        // a follow-up unseal in the same scenario), unseal now so the next scenario
        // can proceed against an operational cluster.
        try {
            Coord.GetSealStatusResponse st = sealStub.getSealStatus(
                    Coord.GetSealStatusRequest.newBuilder().build());
            if (st.getSealed() && !shares.isEmpty()) {
                int need = Math.max(1, st.getThreshold() - (int) st.getProgress());
                for (int i = 0; i < Math.min(need, shares.size()); i++) {
                    sealStub.unseal(Coord.UnsealRequest.newBuilder()
                            .setKeyShare(shares.get(i)).build());
                }
            }
        } catch (io.grpc.StatusRuntimeException e) {
            log.warn("Auto-unseal attempt failed: {}", e.getStatus());
        }

        // Push token to mock services so their gRPC calls can authenticate too.
        try {
            httpClient.pushAuthToken(token);
        } catch (Exception e) {
            log.warn("pushAuthToken best-effort failed: {}", e.getMessage());
        }
    }

    @Before(order = 10)
    public void resetScenarioState() {
        // Reset per-scenario transient fields. Do NOT reset security/transit/PKI fields
        // that are expected to persist across scenarios within the same feature.

        state.clusterStatuses.clear();
        state.discoveredInstances.clear();
        state.generatedIds.clear();
        state.lastConfigResponse = null;

        state.lockResponseA = null;
        state.lockResponseB = null;
        state.lastLockName = null;

        state.workflowId = null;


        state.orderId = null;
        state.orderId2 = null;
        state.productId = null;
        state.initialStock = 0;
        state.lastHttpCode = 0;
        state.lastHttpCode2 = 0;
        state.paymentId = null;
        state.concurrentOrderResults.clear();
        state.orderConfig = null;
        state.lastOrderResponse.clear();

        // Reset mock service in-memory state so that order idempotency caches and
        // payment records from previous scenarios do not bleed into the current one.
        httpClient.resetMockServices();
    }
}
