package cn.byteforce.coord.sdk.workflow;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Serverless Workflow engine API.
 * <p>
 * Provides workflow definition management and instance lifecycle operations
 * backed by Coord's core primitives (KV + Txn + Lease + Watch).
 * <p>
 * <b>Capability level:</b> Full workflow engine support — DSL-driven execution,
 * instance lifecycle (start/signal/cancel), definition CRUD, and filtered listing.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     WorkflowClient wf = client.workflow();
 *
 *     // Deploy a definition, then start by ID
 *     WorkflowDefinition def = wf.deployDefinition("icps", yaml);
 *     String wfId = wf.startByDefinition(def.workflowId(),
 *             "{\"orderId\":\"123\"}".getBytes());
 *
 *     // Check status (includes timestamps + task stack)
 *     WorkflowStatus status = wf.getStatus(wfId);
 *     if (status.state().isTerminal()) {
 *         byte[] output = status.output();
 *     }
 *
 *     // List instances filtered by namespace and status
 *     List<WorkflowInstanceSummary> running = wf.listInstances(
 *             "icps", null, "RUNNING", 20, "");
 * }
 * }</pre>
 */
public interface WorkflowClient {

    // ──── 实例生命周期 ────

    /**
     * Start a new workflow instance (deploy + start in one call).
     * <p>
     * Convenience method: deploys the DSL definition then immediately starts an instance.
     * For repeated starts of the same definition, prefer {@link #startByDefinition(String, byte[])}.
     *
     * @param definitionDsl the workflow definition in CNCF Workflow DSL JSON format
     * @param input         initial input payload for the workflow
     * @return the unique workflow instance ID
     * @throws CoordException on communication failure
     */
    String start(String definitionDsl, byte[] input);

    /**
     * Start a new workflow instance from an already-deployed definition.
     * <p>
     * Use this when you have already called {@link #deployDefinition(String, String)}
     * and want to start additional instances of the same definition.
     *
     * @param definitionId the workflow definition ID (returned by {@link #deployDefinition})
     * @param input        initial input payload for the workflow
     * @return the unique workflow instance ID
     * @throws CoordException on communication failure or definition not found
     */
    String startByDefinition(String definitionId, byte[] input);

    /**
     * Get the current status of a workflow instance.
     *
     * @param workflowId the workflow instance ID returned by {@link #start}
     * @return current workflow status (includes timestamps and task stack)
     * @throws CoordException on communication or not-found failure
     */
    WorkflowStatus getStatus(String workflowId);

    /**
     * Send a signal to a suspended workflow instance.
     *
     * @param workflowId the workflow instance ID
     * @param signalName the signal name (e.g., "approve", "timeout")
     * @param payload    optional signal payload (may be null or empty)
     * @throws CoordException on communication failure
     */
    void signal(String workflowId, String signalName, byte[] payload);

    /**
     * Send a signal with idempotency key.
     * <p>
     * Duplicate signals with the same idempotency key are safely ignored.
     * Use this for payment callbacks, external webhooks, or any scenario
     * where the caller may retry the signal.
     *
     * @param workflowId     the workflow instance ID
     * @param signalName     the signal name (e.g., "approve", "timeout")
     * @param payload        optional signal payload (may be null or empty)
     * @param idempotencyKey unique key to prevent duplicate signal processing
     * @throws CoordException on communication failure
     */
    void signal(String workflowId, String signalName, byte[] payload, String idempotencyKey);

    /**
     * Cancel a running workflow instance.
     *
     * @param workflowId the workflow instance ID
     * @throws CoordException on communication failure
     */
    void cancel(String workflowId);

    // ──── 工作流定义管理 ────

    /**
     * Deploy a workflow definition.
     *
     * @param namespace      the namespace for the workflow
     * @param definitionYaml the workflow definition in YAML format
     * @return the deployed workflow definition info
     * @throws CoordException on deployment or communication failure
     */
    WorkflowDefinition deployDefinition(String namespace, String definitionYaml);

    /**
     * List workflow definitions in a namespace.
     *
     * @param namespace the namespace
     * @return list of workflow definition summaries
     * @throws CoordException on communication failure
     */
    java.util.List<WorkflowDefinitionSummary> listDefinitions(String namespace);

    /**
     * Get a workflow definition by ID.
     *
     * @param workflowId the workflow definition ID
     * @return the full workflow definition (includes raw YAML)
     * @throws CoordException on not found or communication failure
     */
    WorkflowDefinition getDefinition(String workflowId);

    /**
     * List all versions of a workflow definition (rollback target discovery,
     * aligned with policy {@code listBundleVersions}).
     *
     * @param namespace the definition namespace
     * @param name      the definition name
     * @return list of definition versions (semantic version strings, sorted ascending)
     * @throws CoordException on communication failure
     */
    java.util.List<WorkflowDefinitionVersion> listDefinitionVersions(String namespace, String name);

    /**
     * Roll back a workflow definition to a previous version (aligned with policy
     * {@code rollbackBundle}: restore snapshot as a new version, keep enabled state).
     * <p>
     * The target version's DSL is re-validated and restored as a NEW semantic version
     * (target version + 1, e.g. "1.0" → "1.1"); existing versions are kept intact
     * (versioned coexistence). Fetch the full document via {@link #getDefinition}.
     *
     * @param namespace the definition namespace
     * @param name      the definition name
     * @param version   the version to restore (must exist)
     * @return the rolled-back definition summary (definitionYaml is empty; use {@link #getDefinition})
     * @throws CoordException on invalid target version (INVALID_ARGUMENT) or communication failure
     */
    WorkflowDefinition rollbackDefinition(String namespace, String name, String version);

    // ──── 保留策略（G-WF-1）────

    /**
     * Delete a workflow instance (retention policy; capability
     * {@code coord:workflow:execute}).
     * <p>
     * Only <b>terminal</b> instances (completed / faulted / cancelled) can be
     * deleted — non-terminal ones are rejected with
     * {@link cn.byteforce.coord.sdk.ErrorCode#FAILED_PRECONDITION} (cancel it
     * first). Missing instances raise {@code NOT_FOUND}. Deletion is
     * <b>not reversible</b> (no archive copy); there is no automatic TTL.
     *
     * @param workflowId the instance id
     * @throws cn.byteforce.coord.sdk.CoordException on communication failure or
     *                                               guard violation
     */
    void deleteInstance(String workflowId);

    /**
     * Delete a workflow definition version (retention policy; capability
     * {@code coord:workflow:define}).
     * <p>
     * Only allowed when <b>no instance</b> references the
     * {@code (namespace, name, version)} triple — otherwise
     * {@link cn.byteforce.coord.sdk.ErrorCode#FAILED_PRECONDITION} (delete the
     * instances first). After deletion {@link #getDefinition} / rollback to that
     * version return {@code NOT_FOUND}; re-deploying the same version yields a
     * fresh definition. Deletion is <b>not reversible</b>.
     *
     * @param namespace definition namespace
     * @param name      definition name
     * @param version   definition version
     * @throws cn.byteforce.coord.sdk.CoordException on communication failure or
     *                                               guard violation
     */
    void deleteDefinition(String namespace, String name, String version);

    // ──── 工作流实例查询 ────

    /**
     * List workflow instances, optionally filtered by workflow ID.
     *
     * @param workflowId optional workflow ID filter (null or empty for all)
     * @return list of workflow instance summaries
     * @throws CoordException on communication failure
     */
    java.util.List<WorkflowInstanceSummary> listInstances(String workflowId);

    /**
     * List workflow instances with full filtering.
     *
     * @param namespace    optional namespace filter (null or empty for all namespaces)
     * @param workflowId   optional workflow ID filter (null or empty for all definitions)
     * @param statusFilter optional status filter (e.g., "RUNNING", "SUSPENDED", "FAILED"; null or empty for all)
     * @param pageSize     max results per page (1-200; server caps at 200)
     * @param pageToken    pagination token from previous response (empty for first page)
     * @return list of workflow instance summaries (includes namespace, outputJson, contextJson)
     * @throws CoordException on communication failure
     */
    java.util.List<WorkflowInstanceSummary> listInstances(
            String namespace, String workflowId, String statusFilter, int pageSize, String pageToken);

    // ──── 人工审批 v2（ADR-0013 D7）────

    /**
     * Approve the current seat of an approval task.
     *
     * @param workflowId     the workflow instance ID
     * @param taskId         the approval task (seat) ID
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the decision twice
     * @return the instance status after the command (uppercase, e.g. {@code RUNNING}, {@code COMPLETED})
     * @throws CoordException on communication or guard violation
     */
    String approve(String workflowId, String taskId, String comment, String idempotencyKey);

    /**
     * Reject the current seat of an approval task.
     *
     * @param workflowId     the workflow instance ID
     * @param taskId         the approval task (seat) ID
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the decision twice
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication or guard violation
     */
    String reject(String workflowId, String taskId, String comment, String idempotencyKey);

    /**
     * Acknowledge (read receipt) an approval task, advancing it without a decision.
     *
     * @param workflowId     the workflow instance ID
     * @param taskId         the approval task (seat) ID
     * @param idempotencyKey unique key so retries do not apply the ack twice
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication or guard violation
     */
    String acknowledge(String workflowId, String taskId, String idempotencyKey);

    /**
     * Add approvers / pool members / readers to an approval branch (加签).
     *
     * @param workflowId     the workflow instance ID
     * @param branchId       the approval branch ID
     * @param approvers      explicit approver identities to add (may be null/empty)
     * @param pool           pool identities to add (may be null/empty)
     * @param readers        reader identities to add (may be null/empty)
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the change twice
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication or guard violation
     */
    String addSign(String workflowId, String branchId, List<String> approvers,
                   List<String> pool, List<String> readers, String comment, String idempotencyKey);

    /**
     * Remove seats from an approval branch (减签).
     *
     * @param workflowId     the workflow instance ID
     * @param branchId       the approval branch ID
     * @param taskIds        the seat IDs to remove
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the change twice
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication or guard violation
     */
    String removeSign(String workflowId, String branchId, List<String> taskIds,
                      String comment, String idempotencyKey);

    /**
     * Return the approval flow to a previous node (驳回返回到目标节点).
     *
     * @param workflowId     the workflow instance ID
     * @param branchId       the approval branch ID
     * @param targetNode     the node to return to
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the return twice
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication or guard violation
     */
    String returnToNode(String workflowId, String branchId, String targetNode,
                        String comment, String idempotencyKey);

    /**
     * Transfer an approval seat to another assignee.
     *
     * @param workflowId     the workflow instance ID
     * @param taskId         the approval task (seat) ID
     * @param to             the target assignee
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the transfer twice
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication or guard violation
     */
    String transfer(String workflowId, String taskId, String to, String comment, String idempotencyKey);

    /**
     * Delegate an approval seat to another assignee.
     *
     * @param workflowId     the workflow instance ID
     * @param taskId         the approval task (seat) ID
     * @param to             the target assignee
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the delegation twice
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication or guard violation
     */
    String delegate(String workflowId, String taskId, String to, String comment, String idempotencyKey);

    /**
     * Withdraw the whole approval instance (申请人撤回).
     *
     * @param workflowId     the workflow instance ID
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the withdrawal twice
     * @return the instance status after the command (uppercase, e.g. {@code CANCELLED})
     * @throws CoordException on communication or guard violation
     */
    String withdraw(String workflowId, String comment, String idempotencyKey);

    /**
     * Terminate the whole approval instance.
     *
     * @param workflowId     the workflow instance ID
     * @param reason         the termination reason
     * @param idempotencyKey unique key so retries do not apply the termination twice
     * @return the instance status after the command (uppercase, e.g. {@code TERMINATED})
     * @throws CoordException on communication or guard violation
     */
    String terminate(String workflowId, String reason, String idempotencyKey);

    /**
     * Approve as a service acting on behalf of an end user (服务代终端用户).
     *
     * <p>The acting service must be listed in the agent's
     * {@code services.workflow_on_behalf_services}; the represented user must still own
     * the seat. The vote record keeps both identities ({@code actor} = end user,
     * {@code acting_service} = caller).</p>
     *
     * @param workflowId     the workflow instance ID
     * @param taskId         the approval task (seat) ID
     * @param comment        optional comment (may be null)
     * @param idempotencyKey unique key so retries do not apply the decision twice
     * @param onBehalfOf     the represented end user (null/empty = direct operation)
     * @return the instance status after the command (uppercase)
     * @throws CoordException on communication, authorization, or guard violation
     */
    String approveOnBehalfOf(String workflowId, String taskId, String comment,
                             String idempotencyKey, String onBehalfOf);

    /** Reject as a service acting on behalf of an end user (see {@link #approveOnBehalfOf}). */
    String rejectOnBehalfOf(String workflowId, String taskId, String comment,
                            String idempotencyKey, String onBehalfOf);

    /** Acknowledge as a service acting on behalf of an end user. */
    String acknowledgeOnBehalfOf(String workflowId, String taskId,
                                 String idempotencyKey, String onBehalfOf);

    /** Add seats as a service acting on behalf of an end user. */
    String addSignOnBehalfOf(String workflowId, String branchId, List<String> approvers,
                             List<String> pool, List<String> readers, String comment,
                             String idempotencyKey, String onBehalfOf);

    /** Remove seats as a service acting on behalf of an end user. */
    String removeSignOnBehalfOf(String workflowId, String branchId, List<String> taskIds,
                                String comment, String idempotencyKey, String onBehalfOf);

    /** Return to a previous node as a service acting on behalf of an end user. */
    String returnToNodeOnBehalfOf(String workflowId, String branchId, String targetNode,
                                  String comment, String idempotencyKey, String onBehalfOf);

    /** Transfer a seat as a service acting on behalf of the current assignee. */
    String transferOnBehalfOf(String workflowId, String taskId, String to, String comment,
                              String idempotencyKey, String onBehalfOf);

    /** Delegate a seat as a service acting on behalf of the current assignee. */
    String delegateOnBehalfOf(String workflowId, String taskId, String to, String comment,
                              String idempotencyKey, String onBehalfOf);

    /** Withdraw as a service acting on behalf of the initiator. */
    String withdrawOnBehalfOf(String workflowId, String comment, String idempotencyKey,
                              String onBehalfOf);

    /** Terminate as a service acting on behalf of a sign admin. */
    String terminateOnBehalfOf(String workflowId, String reason, String idempotencyKey,
                               String onBehalfOf);

    /**
     * List pending approval seats (待办) for the current principal.
     *
     * @param pageSize  max results per page (1-200; server caps at 200)
     * @param pageToken pagination token from previous response (empty for first page)
     * @return list of pending approval seats
     * @throws CoordException on communication failure
     */
    List<PendingApprovalTask> listPendingTasks(int pageSize, String pageToken);

    /**
     * List pending approval seats for a specific assignee (服务代终端用户查询待办).
     *
     * <p>When {@code assignee} differs from the caller, the caller must be listed in
     * {@code services.workflow_on_behalf_services}; otherwise the server returns
     * {@code PERMISSION_DENIED}.</p>
     *
     * @param assignee  the end user whose pending seats are queried (null/empty = caller)
     * @param pageSize  max results per page (1-200; server caps at 200)
     * @param pageToken pagination token from previous response (empty for first page)
     * @return list of pending approval seats
     * @throws CoordException on communication or authorization failure
     */
    List<PendingApprovalTask> listPendingTasksFor(String assignee, int pageSize, String pageToken);

    /**
     * Get the full approval detail (branches / tasks / rounds) of an instance.
     *
     * @param workflowId the workflow instance ID
     * @return the approval detail (includes revision for optimistic concurrency)
     * @throws CoordException on not found or communication failure
     */
    ApprovalInstanceDetail getInstanceDetail(String workflowId);

    // ──── 异步回调 ────

    /**
     * Start a workflow and return a future that completes when the workflow
     * reaches a terminal state.
     * <p>
     * Uses Coord Watch internally — zero polling in the happy path.
     * Falls back to polling if the watch stream disconnects.
     * <p>
     * Usage:
     * <pre>{@code
     * wf.startAsync(defId, input)
     *     .thenAccept(status -> {
     *         if (status.state() == WorkflowState.COMPLETED) {
     *             processResult(status.output());
     *         }
     *     });
     *
     * // With timeout:
     * WorkflowStatus status = wf.startAsync(defId, input)
     *     .orTimeout(30, TimeUnit.SECONDS)
     *     .get();
     * }</pre>
     *
     * @param definitionId the workflow definition ID (returned by {@link #deployDefinition})
     * @param input        initial input payload for the workflow
     * @return a CompletableFuture that resolves to the final WorkflowStatus
     * @throws CoordException on communication failure during start
     */
    CompletableFuture<WorkflowStatus> startAsync(String definitionId, byte[] input);

    /**
     * Watch an existing workflow instance for completion.
     * <p>
     * Creates a KV Watch on the instance key and completes the future
     * when the instance reaches a terminal state. If the watch stream
     * disconnects, automatically falls back to polling {@link #getStatus}.
     *
     * @param instanceId the workflow instance ID
     * @return a CompletableFuture that resolves to the final WorkflowStatus
     * @throws CoordException on communication failure during initial status check
     */
    CompletableFuture<WorkflowStatus> watchInstance(String instanceId);
}
