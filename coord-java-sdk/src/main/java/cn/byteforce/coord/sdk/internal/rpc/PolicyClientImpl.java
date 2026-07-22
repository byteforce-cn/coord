package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.PolicyCheckPermissionRequest;
import cn.byteforce.coord.sdk.internal.proto.PolicyCheckPermissionResponse;
import cn.byteforce.coord.sdk.internal.proto.PolicyEvaluateRequest;
import cn.byteforce.coord.sdk.internal.proto.PolicyEvaluateResponse;
import cn.byteforce.coord.sdk.internal.proto.PolicyExplainRequest;
import cn.byteforce.coord.sdk.internal.proto.PolicyExplainResponse;
import cn.byteforce.coord.sdk.internal.proto.PolicyPutBundleRequest;
import cn.byteforce.coord.sdk.internal.proto.PolicyPutBundleResponse;
import cn.byteforce.coord.sdk.internal.proto.PolicyDeleteBundleRequest;
import cn.byteforce.coord.sdk.internal.proto.PolicyDeleteBundleResponse;
import cn.byteforce.coord.sdk.internal.proto.PolicyListBundlesRequest;
import cn.byteforce.coord.sdk.internal.proto.PolicyListBundlesResponse;
import cn.byteforce.coord.sdk.internal.proto.PolicySetBundleEnabledRequest;
import cn.byteforce.coord.sdk.internal.proto.PolicySetBundleEnabledResponse;
import cn.byteforce.coord.sdk.internal.proto.PolicyGrpc;
import cn.byteforce.coord.sdk.policy.PolicyClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link PolicyClient} backed by gRPC calls to the Coord Agent.
 */
public final class PolicyClientImpl extends AgentRpcClient implements PolicyClient {

    private static final Logger log = LoggerFactory.getLogger(PolicyClientImpl.class);
    private final CoordConfig config;

    public PolicyClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                            RetryTemplate retryTemplate, ObservabilityProvider observability,
                            CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public boolean checkPermission(String principal, String resource,
                                   String action, byte[] context) {
        PolicyCheckPermissionRequest.Builder req = PolicyCheckPermissionRequest.newBuilder()
                .setPrincipal(principal)
                .setResource(resource)
                .setAction(action);
        if (context != null && context.length > 0) {
            req.setContext(ByteString.copyFrom(context));
        }

        PolicyCheckPermissionResponse response = callWithRetry(
                (ch, r) -> PolicyGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .checkPermission((PolicyCheckPermissionRequest) r),
                req.build(), "policy.checkPermission");

        boolean allowed = response.getAllowed();
        log.debug("Policy check: principal={}, resource={}, action={}, allowed={}, reason={}",
                principal, resource, action, allowed, response.getReason());
        return allowed;
    }

    @Override
    public byte[] evaluate(String query, byte[] input) {
        PolicyEvaluateRequest.Builder req = PolicyEvaluateRequest.newBuilder()
                .setQuery(query);
        if (input != null && input.length > 0) {
            req.setInput(ByteString.copyFrom(input));
        }

        PolicyEvaluateResponse response = callWithRetry(
                (ch, r) -> PolicyGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .evaluate((PolicyEvaluateRequest) r),
                req.build(), "policy.evaluate");

        byte[] result = response.getResult().toByteArray();
        log.debug("Policy evaluate: query={}, result_len={}", query, result.length);
        return result;
    }

    @Override
    public byte[] explain(String tenantId, String namespace, String inputJson) {
        PolicyExplainRequest.Builder req = PolicyExplainRequest.newBuilder()
                .setQuery("data." + namespace + ".allow");
        if (inputJson != null && !inputJson.isEmpty()) {
            req.setInput(ByteString.copyFromUtf8(inputJson));
        }

        PolicyExplainResponse response = callWithRetry(
                (ch, r) -> PolicyGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .explain((PolicyExplainRequest) r),
                req.build(), "policy.explain");

        byte[] trace = response.getTrace().toByteArray();
        log.debug("Policy explain: tenant={}, ns={}, trace_len={}", tenantId, namespace, trace.length);
        return trace;
    }

    @Override
    public PolicyClient.BundleInfo putBundle(String tenantId, String namespace,
                                              String name, String regoContent) {
        PolicyPutBundleRequest req = PolicyPutBundleRequest.newBuilder()
                .setTenantId(tenantId)
                .setNamespace(namespace)
                .setName(name)
                .setRegoContent(regoContent)
                .build();

        PolicyPutBundleResponse response = callWithRetry(
                (ch, r) -> PolicyGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .putBundle((PolicyPutBundleRequest) r),
                req, "policy.putBundle");

        log.debug("Policy putBundle: name={}, bundleId={}", name, response.getBundleId());
        return new PolicyClient.BundleInfo(
                response.getBundleId(),
                response.getName(),
                response.getNamespace(),
                tenantId,
                response.getEnabled(),
                response.getCreatedAt(),
                response.getUpdatedAt());
    }

    @Override
    public void deleteBundle(String bundleId) {
        PolicyDeleteBundleRequest req = PolicyDeleteBundleRequest.newBuilder()
                .setBundleId(bundleId)
                .build();

        callWithRetry(
                (ch, r) -> PolicyGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .deleteBundle((PolicyDeleteBundleRequest) r),
                req, "policy.deleteBundle");

        log.debug("Policy deleteBundle: bundleId={}", bundleId);
    }

    @Override
    public void setBundleEnabled(String bundleId, boolean enabled) {
        PolicySetBundleEnabledRequest req = PolicySetBundleEnabledRequest.newBuilder()
                .setBundleId(bundleId)
                .setEnabled(enabled)
                .build();

        callWithRetry(
                (ch, r) -> PolicyGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .setBundleEnabled((PolicySetBundleEnabledRequest) r),
                req, "policy.setBundleEnabled");

        log.debug("Policy setBundleEnabled: bundleId={}, enabled={}", bundleId, enabled);
    }

    @Override
    public List<PolicyClient.BundleInfo> listBundles(String tenantId) {
        PolicyListBundlesRequest.Builder reqBuilder = PolicyListBundlesRequest.newBuilder();
        if (tenantId != null && !tenantId.isEmpty()) {
            reqBuilder.setTenantId(tenantId);
        }

        PolicyListBundlesResponse response = callWithRetry(
                (ch, r) -> PolicyGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .listBundles((PolicyListBundlesRequest) r),
                reqBuilder.build(), "policy.listBundles");

        List<PolicyClient.BundleInfo> bundles = new ArrayList<>();
        for (var pb : response.getBundlesList()) {
            bundles.add(new PolicyClient.BundleInfo(
                    pb.getBundleId(),
                    pb.getName(),
                    pb.getNamespace(),
                    pb.getTenantId(),
                    pb.getEnabled(),
                    pb.getCreatedAt(),
                    pb.getUpdatedAt()));
        }
        log.debug("Policy listBundles: tenant={}, count={}", tenantId, bundles.size());
        return bundles;
    }
}
