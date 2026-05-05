package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试场景间共享状态容器（Cucumber World 对象）。
 */
@Component
public class ScenarioState {
    // Cluster
    public List<Coord.ClusterStatusResponse> clusterStatuses = new ArrayList<>();
    public String lastCiphertext;

    // Registry
    public String registeredLeaseId;
    public List<Coord.ServiceInstance> discoveredInstances = new ArrayList<>();

    // Config
    public Coord.ConfigResponse lastConfigResponse;

    // Lock
    public Coord.LockAcquireResponse lockResponseA;
    public Coord.LockAcquireResponse lockResponseB;
    public String lastLockName;

    // IdGen
    public List<Long> generatedIds = new ArrayList<>();

    // Workflow (shared)
    public String workflowId;

    // Workflow v2
    public String workflowDefId;
    public String workflowDefVersion;
    public String workflowInstanceId;
    public Coord.ListWorkflowDefinitionsResponse workflowDefList;
    public Coord.ListWorkflowInstancesResponse workflowInstanceList;

    // Transit
    public String transitKeyName;
    public String transitCiphertext;
    public String transitCiphertextAfterRotation;
    public String transitPlaintext;
    public String transitDecrypted;
    public String transitHmac;
    public boolean transitVerifyValid;
    public Coord.GetTransitKeyResponse transitKeyInfo;

    // PKI
    public String pkiRole;
    public String pkiCertPem;
    public String pkiSerialNumber;
    public String pkiCaChain;
    public String pkiCrl;
    public String pkiCertStatus;
    public String acmeOrderId;
    public String acmeChallengeToken;
    public String acmeDomain;
    public Coord.RunAutoRenewResponse autoRenewResponse;

    // Security (Seal + Auth)
    public List<String> unsealShares = new ArrayList<>();
    public List<String> newUnsealShares = new ArrayList<>();
    public boolean rotateRootKeySuccess;
    public String sealRootToken;
    public String appRoleName;
    public String authToken;
    public String secretId;
    public Coord.LookupTokenResponse lookupTokenResponse;
    public io.grpc.StatusRuntimeException lastLoginError;
    public boolean lastPermDenied;

    // Order flow
    public String orderId;
    public String orderId2;
    public String productId;
    public int initialStock;
    public int lastHttpCode;
    public int lastHttpCode2;
    public String paymentId;
    public List<Integer> concurrentOrderResults = new ArrayList<>();
    public String orderConfig;
    public Map<String, Object> lastOrderResponse = new HashMap<>();

    // Cluster failover
    public String lastConfigWriteKey;
    public String lastConfigWriteValue;
    public boolean lastConfigWriteSucceeded = true;

    public void reset() {
        clusterStatuses.clear();
        lastCiphertext = null;
        discoveredInstances.clear();
        generatedIds.clear();
        unsealShares.clear();
        concurrentOrderResults.clear();
        lastConfigResponse = null;
        lockResponseA = null;
        lockResponseB = null;
        lastLockName = null;
        workflowId = null;
        workflowDefId = null;
        workflowDefVersion = null;
        workflowInstanceId = null;
        workflowDefList = null;
        workflowInstanceList = null;
        transitKeyName = null;
        transitCiphertext = null;
        transitCiphertextAfterRotation = null;
        transitPlaintext = null;
        transitDecrypted = null;
        transitHmac = null;
        transitVerifyValid = false;
        transitKeyInfo = null;
        pkiRole = null;
        pkiCertPem = null;
        pkiSerialNumber = null;
        pkiCaChain = null;
        pkiCrl = null;
        pkiCertStatus = null;
        acmeOrderId = null;
        acmeChallengeToken = null;
        newUnsealShares.clear();
        rotateRootKeySuccess = false;
        sealRootToken = null;
        appRoleName = null;
        authToken = null;
        secretId = null;
        lookupTokenResponse = null;
        lastLoginError = null;
        lastPermDenied = false;
        orderId = null;
        orderId2 = null;
        productId = null;
        initialStock = 0;
        lastHttpCode = 0;
        lastHttpCode2 = 0;
        paymentId = null;
        orderConfig = null;
        lastOrderResponse.clear();
        lastConfigWriteKey = null;
        lastConfigWriteValue = null;
        lastConfigWriteSucceeded = true;
    }
}
