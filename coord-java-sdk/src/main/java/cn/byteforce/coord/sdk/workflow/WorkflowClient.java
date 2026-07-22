package cn.byteforce.coord.sdk.workflow;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Serverless Workflow engine API.
 * <p>
 * Provides workflow definition management and instance lifecycle operations
 * backed by Coord's core primitives (KV + Txn + Lease + Watch).
 * <p>
 * <b>Current capability level (Phase D):</b> Workflow definition CRUD and
 * instance state management. Full DSL interpretation and Saga compensation
 * execution are Phase G roadmap items.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     WorkflowClient wf = client.workflow();
 *
 *     // Start a workflow
 *     String wfId = wf.start("{\"name\":\"order-proc\",\"steps\":[...]}",
 *             "{\"orderId\":\"123\"}".getBytes());
 *
 *     // Check status
 *     WorkflowStatus status = wf.getStatus(wfId);
 *     if (status.state().isTerminal()) {
 *         byte[] output = status.output();
 *     }
 * }
 * }</pre>
 */
public interface WorkflowClient {

    /**
     * Start a new workflow instance.
     *
     * @param definitionDsl the workflow definition in CNCF Workflow DSL JSON format
     * @param input         initial input payload for the workflow
     * @return the unique workflow instance ID
     * @throws CoordException on communication failure
     */
    String start(String definitionDsl, byte[] input);

    /**
     * Get the current status of a workflow instance.
     *
     * @param workflowId the workflow instance ID returned by {@link #start}
     * @return current workflow status
     * @throws CoordException on communication or not-found failure
     */
    WorkflowStatus getStatus(String workflowId);

    /**
     * Send a signal to a running workflow instance.
     *
     * @param workflowId the workflow instance ID
     * @param signalName the signal name (e.g., "approve", "timeout")
     * @param payload    optional signal payload
     * @throws CoordException on communication failure
     */
    void signal(String workflowId, String signalName, byte[] payload);

    /**
     * Cancel a running workflow instance.
     *
     * @param workflowId the workflow instance ID
     * @throws CoordException on communication failure
     */
    void cancel(String workflowId);

    // ──── 工作流定义管理 (Phase C.2) ────

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
     * @return the full workflow definition
     * @throws CoordException on not found or communication failure
     */
    WorkflowDefinition getDefinition(String workflowId);

    /**
     * List workflow instances, optionally filtered by workflow ID.
     *
     * @param workflowId optional workflow ID filter (empty for all)
     * @return list of workflow instance summaries
     * @throws CoordException on communication failure
     */
    java.util.List<WorkflowInstanceSummary> listInstances(String workflowId);
}
