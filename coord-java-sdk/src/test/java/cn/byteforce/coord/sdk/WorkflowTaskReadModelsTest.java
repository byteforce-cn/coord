package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.workflow.ApprovalAssignment;
import cn.byteforce.coord.sdk.workflow.ApprovalHistoryEvent;
import cn.byteforce.coord.sdk.workflow.ApprovalTask;
import cn.byteforce.coord.sdk.workflow.ApprovalVote;
import cn.byteforce.coord.sdk.workflow.TaskDetail;
import cn.byteforce.coord.sdk.workflow.TaskPage;
import cn.byteforce.coord.sdk.workflow.TaskQueryFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Model tests for the workflow read surface ({@code listTasks} / {@code getTask} /
 * {@code history}): the shared {@link ApprovalTask} model, the page/detail wrappers
 * and backward compatibility of the pre-existing constructors.
 */
class WorkflowTaskReadModelsTest {

    private static final ApprovalVote VOTE =
            new ApprovalVote("rd-1", "approve", "alice", "", "ok", 1755000000000L, "svc-bff");

    @Test
    @DisplayName("ApprovalTask 完整构造器暴露读面字段（instance/node/origin/图链接/assignments）")
    void fullConstructorExposesReadSurfaceFields() {
        ApprovalAssignment assignment = new ApprovalAssignment("alice", "completed", "voted", VOTE);
        ApprovalTask t = new ApprovalTask("task-9", "br-1", "rd-1", "APPROVAL", "completed",
                "alice", List.of("alice", "bob"), 1755000000000L, List.of(VOTE),
                "inst-1", "approve", "APPROVAL", 1754000000000L,
                "task-1", "task-10", "act-7", List.of(assignment));

        assertThat(t.id()).isEqualTo("task-9");
        assertThat(t.instanceId()).isEqualTo("inst-1");
        assertThat(t.node()).isEqualTo("approve");
        assertThat(t.origin()).isEqualTo("APPROVAL");
        assertThat(t.createdAtMs()).isEqualTo(1754000000000L);
        assertThat(t.predecessorTaskId()).isEqualTo("task-1");
        assertThat(t.successorTaskId()).isEqualTo("task-10");
        assertThat(t.spawnedByActionId()).isEqualTo("act-7");
        assertThat(t.votes()).containsExactly(VOTE);
        assertThat(t.assignments()).containsExactly(assignment);
        assertThat(t.assignments().get(0).vote().actingService()).isEqualTo("svc-bff");
    }

    @Test
    @DisplayName("既有 8/9 参构造器保持兼容：读面字段为空、集合不可变")
    void legacyConstructorsStayCompatible() {
        List<String> candidates = new ArrayList<>(List.of("alice"));
        ApprovalTask t = new ApprovalTask("task-9", "br-1", "rd-1", "APPROVAL", "created",
                "alice", candidates, 0L);

        assertThat(t.instanceId()).isEmpty();
        assertThat(t.node()).isEmpty();
        assertThat(t.origin()).isEmpty();
        assertThat(t.createdAtMs()).isZero();
        assertThat(t.predecessorTaskId()).isEmpty();
        assertThat(t.successorTaskId()).isEmpty();
        assertThat(t.spawnedByActionId()).isEmpty();
        assertThat(t.votes()).isEmpty();
        assertThat(t.assignments()).isEmpty();

        // 既有契约：只读视图（非防御性拷贝）——构造后修改源列表会透出，但不可直接改视图。
        assertThatThrownBy(() -> t.candidates().add("bob"))
                .isInstanceOf(UnsupportedOperationException.class);

        ApprovalTask legacyWithVotes = new ApprovalTask("task-9", "br-1", "rd-1", "APPROVAL",
                "created", "alice", List.of("alice"), 0L, List.of(VOTE));
        assertThat(legacyWithVotes.votes()).containsExactly(VOTE);
        assertThat(legacyWithVotes.instanceId()).isEmpty();
    }

    @Test
    @DisplayName("ApprovalTask equals/hashCode 覆盖读面字段")
    void approvalTaskEqualityCoversReadSurfaceFields() {
        ApprovalTask base = task("task-9", "APPROVAL");
        ApprovalTask same = task("task-9", "APPROVAL");
        ApprovalTask otherOrigin = task("task-9", "ASSIGNEE");

        assertThat(base).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(base).isNotEqualTo(otherOrigin);
        assertThat(base.toString()).contains("task-9").contains("inst-1").contains("approve");
    }

    @Test
    @DisplayName("TaskPage 复制列表并把空 token 归一为空串")
    void taskPageIsDefensiveAndNormalizesToken() {
        List<ApprovalTask> tasks = new ArrayList<>(List.of(task("task-9", "APPROVAL")));
        TaskPage page = new TaskPage(tasks, null);

        assertThat(page.nextPageToken()).isEmpty();
        tasks.clear();
        assertThat(page.tasks()).hasSize(1);

        assertThat(new TaskPage(null, "next-2").tasks()).isEmpty();
        assertThat(new TaskPage(null, "next-2").nextPageToken()).isEqualTo("next-2");
    }

    @Test
    @DisplayName("TaskDetail 组合席位与实例状态")
    void taskDetailComposesTaskAndInstanceStatus() {
        ApprovalTask seat = task("task-9", "APPROVAL");
        TaskDetail detail = new TaskDetail(seat, "COMPLETED");

        assertThat(detail.task()).isSameAs(seat);
        assertThat(detail.instanceStatus()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("ApprovalHistoryEvent 保留原始 attributesJson")
    void historyEventKeepsRawAttributesJson() {
        String raw = "{\"b\": 1,   \"a\": [2, 3]}";
        ApprovalHistoryEvent e = new ApprovalHistoryEvent("inst-1", "ev-1", "TASK_URGED",
                "approve", "task-9", "", 1755000000000L, raw);

        assertThat(e.attributesJson()).isEqualTo(raw);
        assertThat(e.node()).isEqualTo("approve");
        assertThat(e.actionId()).isEmpty();
        assertThat(e).isEqualTo(new ApprovalHistoryEvent("inst-1", "ev-1", "TASK_URGED",
                "approve", "task-9", "", 1755000000000L, raw));
    }

    @Test
    @DisplayName("TaskQueryFilter 组件可空（null = 不过滤）")
    void taskQueryFilterAllowsNullDimensions() {
        TaskQueryFilter filter = new TaskQueryFilter(
                null, "alice", null, null, "approve", null, 1756000000000L);

        assertThat(filter.instanceId()).isNull();
        assertThat(filter.assignee()).isEqualTo("alice");
        assertThat(filter.dueBeforeMs()).isEqualTo(1756000000000L);
        assertThat(new TaskQueryFilter(null, null, null, null, null, null, null).status()).isNull();
    }

    private static ApprovalTask task(String id, String origin) {
        return new ApprovalTask(id, "br-1", "rd-1", "APPROVAL", "claimed", "alice",
                List.of("alice"), 1755000000000L, List.of(VOTE),
                "inst-1", "approve", origin, 1754000000000L,
                "", "", "", List.of(new ApprovalAssignment("alice", "pending", "", null)));
    }
}
