package cn.byteforce.coord.example;

import cn.byteforce.coord.contracts.workflow.v1.PendingApprovalTask;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGrpc;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 人工审批 v2 — Java 应用通过原始 gRPC stub 直连 Coord Agent。
 *
 * 演示审批 v2 的两条主路径：
 * 1. {@code ListPendingTasks}：拉取当前主体的待办席位；
 * 2. {@code Approve}：对其中一个席位作出“同意”决定。
 *
 * <p>本示例只依赖 coord-proto 生成的 gRPC stub，不引入 coord-java-sdk
 * （与 {@link CoordClient} 同一约定）。命令类 RPC 都需要
 * {@code idempotency_key}：网络重试时服务端据此只落一次状态与事件。
 *
 * 用法:
 * <pre>{@code
 *   WorkflowApprovals approvals = WorkflowApprovals.connect("localhost", 19527);
 *   List<PendingApprovalTask> tasks = approvals.listPendingTasks(20, "");
 *   if (!tasks.isEmpty()) {
 *       PendingApprovalTask t = tasks.get(0);
 *       String status = approvals.approve(
 *           t.getInstanceId(), t.getTaskId(), "同意", "req-42");
 *   }
 *   approvals.close();
 * }</pre>
 */
public class WorkflowApprovals implements AutoCloseable {

    private final ManagedChannel channel;
    private final WorkflowGrpc.WorkflowBlockingStub workflowStub;

    private WorkflowApprovals(String host, int port) {
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
        this.workflowStub = WorkflowGrpc.newBlockingStub(channel);
    }

    /**
     * 创建连接本地 Agent 的审批客户端。
     *
     * @param host Agent 地址（通常 localhost）
     * @param port Agent gRPC 端口（默认 19527）
     */
    public static WorkflowApprovals connect(String host, int port) {
        return new WorkflowApprovals(host, port);
    }

    /**
     * 创建连接本地 Agent 的审批客户端（使用默认端口 19527）。
     */
    public static WorkflowApprovals connectToLocalAgent() {
        return new WorkflowApprovals("localhost", 19527);
    }

    /**
     * 拉取当前主体的待办审批席位。
     *
     * @param pageSize  每页条数（1-200）
     * @param pageToken 上一页返回的分页 token（首页传空串）
     */
    public List<PendingApprovalTask> listPendingTasks(int pageSize, String pageToken) {
        WorkflowListPendingTasksRequest.Builder req = WorkflowListPendingTasksRequest.newBuilder()
                .setPageSize(pageSize);
        if (pageToken != null && !pageToken.isEmpty()) {
            req.setPageToken(pageToken);
        }
        WorkflowListPendingTasksResponse resp = workflowStub.listPendingTasks(req.build());
        return resp.getTasksList();
    }

    /**
     * 对指定席位作出“同意”决定。
     *
     * @param workflowId     工作流实例 ID
     * @param taskId         审批席位 ID
     * @param comment        审批意见
     * @param idempotencyKey 幂等键（重试时服务端据此去重）
     * @return 命令后的实例状态（大写，如 {@code RUNNING} / {@code COMPLETED}）
     */
    public String approve(String workflowId, String taskId, String comment,
                          String idempotencyKey) {
        WorkflowApproveResponse resp = workflowStub.approve(WorkflowApproveRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskId(taskId)
                .setComment(comment)
                .setIdempotencyKey(idempotencyKey)
                .build());
        return resp.getStatus();
    }

    @Override
    public void close() {
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }
}
