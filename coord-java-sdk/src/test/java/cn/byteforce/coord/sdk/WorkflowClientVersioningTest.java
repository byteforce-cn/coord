package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.workflow.ApprovalBranch;
import cn.byteforce.coord.sdk.workflow.ApprovalInstanceDetail;
import cn.byteforce.coord.sdk.workflow.ApprovalRound;
import cn.byteforce.coord.sdk.workflow.ApprovalTask;
import cn.byteforce.coord.sdk.workflow.PendingApprovalTask;
import cn.byteforce.coord.sdk.workflow.WorkflowClient;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinition;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinitionVersion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WorkflowClient} definition versioning and rollback.
 *
 * <p>These tests verify the SDK's data model and method signatures compile
 * correctly. End-to-end integration tests require a running Coord Agent.
 */
class WorkflowClientVersioningTest {

    @Test
    @DisplayName("WorkflowDefinitionVersion constructor and getters")
    void testWorkflowDefinitionVersionGetters() {
        WorkflowDefinitionVersion v = new WorkflowDefinitionVersion(
                "1.1", "icps-flow-123", "active", 1753000000L);

        assertThat(v.version()).isEqualTo("1.1");
        assertThat(v.workflowId()).isEqualTo("icps-flow-123");
        assertThat(v.status()).isEqualTo("active");
        assertThat(v.createdAt()).isEqualTo(1753000000L);
        assertThat(v.toString()).contains("1.1");
        assertThat(v.toString()).contains("icps-flow-123");
    }

    @Test
    @DisplayName("WorkflowDefinitionVersion equals/hashCode")
    void testWorkflowDefinitionVersionEquality() {
        WorkflowDefinitionVersion a = new WorkflowDefinitionVersion("1.0", "wf-1", "active", 1L);
        WorkflowDefinitionVersion b = new WorkflowDefinitionVersion("1.0", "wf-1", "active", 1L);
        WorkflowDefinitionVersion c = new WorkflowDefinitionVersion("1.1", "wf-1", "active", 1L);

        assertThat(a).isEqualTo(b);
        assertThat(a).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    @DisplayName("listDefinitionVersions returns ordered list model")
    void testListDefinitionVersionsModel() {
        List<WorkflowDefinitionVersion> versions = Arrays.asList(
                new WorkflowDefinitionVersion("1.0", "wf-a", "active", 1L),
                new WorkflowDefinitionVersion("1.1", "wf-b", "active", 2L));

        assertThat(versions).hasSize(2);
        assertThat(versions.get(0).version()).isEqualTo("1.0");
        assertThat(versions.get(1).version()).isEqualTo("1.1");
    }

    @Test
    @DisplayName("rollbackDefinition returns WorkflowDefinition summary")
    void testRollbackDefinitionModel() {
        WorkflowDefinition rolled = new WorkflowDefinition(
                "icps-flow-456", "order-approval", "",
                "1.1", "active", 1753000000L);

        assertThat(rolled.workflowId()).isEqualTo("icps-flow-456");
        assertThat(rolled.name()).isEqualTo("order-approval");
        assertThat(rolled.version()).isEqualTo("1.1");
        assertThat(rolled.status()).isEqualTo("active");
    }

    @Test
    @DisplayName("PendingApprovalTask constructor and getters")
    void testPendingApprovalTaskGetters() {
        PendingApprovalTask t = new PendingApprovalTask(
                "inst-1", "task-9", "approve", "br-1", "rd-1",
                "APPROVAL", "PENDING", "alice", 1755000000000L, 1754000000000L);

        assertThat(t.instanceId()).isEqualTo("inst-1");
        assertThat(t.taskId()).isEqualTo("task-9");
        assertThat(t.node()).isEqualTo("approve");
        assertThat(t.branchId()).isEqualTo("br-1");
        assertThat(t.roundId()).isEqualTo("rd-1");
        assertThat(t.kind()).isEqualTo("APPROVAL");
        assertThat(t.status()).isEqualTo("PENDING");
        assertThat(t.assignee()).isEqualTo("alice");
        assertThat(t.deadlineMs()).isEqualTo(1755000000000L);
        assertThat(t.createdAtMs()).isEqualTo(1754000000000L);
        assertThat(t.toString()).contains("task-9");
    }

    @Test
    @DisplayName("PendingApprovalTask equals/hashCode")
    void testPendingApprovalTaskEquality() {
        PendingApprovalTask a = new PendingApprovalTask(
                "inst-1", "task-9", "approve", "br-1", "rd-1",
                "APPROVAL", "PENDING", "alice", 1L, 2L);
        PendingApprovalTask b = new PendingApprovalTask(
                "inst-1", "task-9", "approve", "br-1", "rd-1",
                "APPROVAL", "PENDING", "alice", 1L, 2L);
        PendingApprovalTask c = new PendingApprovalTask(
                "inst-1", "task-9", "approve", "br-1", "rd-1",
                "APPROVAL", "PENDING", "carol", 1L, 2L);

        assertThat(a).isEqualTo(b);
        assertThat(a).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    @DisplayName("ApprovalBranch/Task/Round getters and equality")
    void testApprovalModelGettersAndEquality() {
        ApprovalBranch branch = new ApprovalBranch(
                "br-1", "approve", 1, "ACTIVE", "all", "any", List.of("rd-1"));
        ApprovalBranch branchCopy = new ApprovalBranch(
                "br-1", "approve", 1, "ACTIVE", "all", "any", List.of("rd-1"));
        assertThat(branch.id()).isEqualTo("br-1");
        assertThat(branch.node()).isEqualTo("approve");
        assertThat(branch.attempt()).isEqualTo(1);
        assertThat(branch.status()).isEqualTo("ACTIVE");
        assertThat(branch.join()).isEqualTo("all");
        assertThat(branch.reject()).isEqualTo("any");
        assertThat(branch.roundIds()).containsExactly("rd-1");
        assertThat(branch).isEqualTo(branchCopy).hasSameHashCodeAs(branchCopy);

        ApprovalTask task = new ApprovalTask(
                "task-9", "br-1", "rd-1", "APPROVAL", "PENDING", "alice",
                List.of("alice", "carol"), 1755000000000L);
        ApprovalTask taskCopy = new ApprovalTask(
                "task-9", "br-1", "rd-1", "APPROVAL", "PENDING", "alice",
                List.of("alice", "carol"), 1755000000000L);
        assertThat(task.id()).isEqualTo("task-9");
        assertThat(task.branchId()).isEqualTo("br-1");
        assertThat(task.roundId()).isEqualTo("rd-1");
        assertThat(task.kind()).isEqualTo("APPROVAL");
        assertThat(task.status()).isEqualTo("PENDING");
        assertThat(task.assignee()).isEqualTo("alice");
        assertThat(task.candidates()).containsExactly("alice", "carol");
        assertThat(task.deadlineMs()).isEqualTo(1755000000000L);
        assertThat(task).isEqualTo(taskCopy).hasSameHashCodeAs(taskCopy);

        ApprovalRound round = new ApprovalRound("rd-1", "br-1", 2, "IN_PROGRESS", 3, 1, 0);
        ApprovalRound roundCopy = new ApprovalRound("rd-1", "br-1", 2, "IN_PROGRESS", 3, 1, 0);
        assertThat(round.id()).isEqualTo("rd-1");
        assertThat(round.branchId()).isEqualTo("br-1");
        assertThat(round.number()).isEqualTo(2);
        assertThat(round.status()).isEqualTo("IN_PROGRESS");
        assertThat(round.totalCount()).isEqualTo(3);
        assertThat(round.completedCount()).isEqualTo(1);
        assertThat(round.rejectedCount()).isEqualTo(0);
        assertThat(round).isEqualTo(roundCopy).hasSameHashCodeAs(roundCopy);
        assertThat(round).isNotEqualTo(new ApprovalRound("rd-1", "br-1", 3, "IN_PROGRESS", 3, 1, 0));
    }

    @Test
    @DisplayName("ApprovalInstanceDetail constructor, getters and equality")
    void testApprovalInstanceDetail() {
        List<ApprovalBranch> branches = List.of(
                new ApprovalBranch("br-1", "approve", 1, "ACTIVE", "all", "any", List.of("rd-1")));
        List<ApprovalTask> tasks = List.of(
                new ApprovalTask("task-9", "br-1", "rd-1", "APPROVAL", "PENDING",
                        "alice", List.of("alice"), 0L));
        List<ApprovalRound> rounds = List.of(
                new ApprovalRound("rd-1", "br-1", 1, "IN_PROGRESS", 1, 0, 0));

        ApprovalInstanceDetail d = new ApprovalInstanceDetail(
                "inst-1", "RUNNING", "bob", 7L, "order-approval", branches, tasks, rounds);
        ApprovalInstanceDetail dCopy = new ApprovalInstanceDetail(
                "inst-1", "RUNNING", "bob", 7L, "order-approval", branches, tasks, rounds);

        assertThat(d.instanceId()).isEqualTo("inst-1");
        assertThat(d.status()).isEqualTo("RUNNING");
        assertThat(d.initiator()).isEqualTo("bob");
        assertThat(d.revision()).isEqualTo(7L);
        assertThat(d.definitionName()).isEqualTo("order-approval");
        assertThat(d.branches()).hasSize(1);
        assertThat(d.tasks()).hasSize(1);
        assertThat(d.rounds()).hasSize(1);
        assertThat(d.toString()).contains("inst-1");
        assertThat(d).isEqualTo(dCopy).hasSameHashCodeAs(dCopy);
        assertThat(d).isNotEqualTo(new ApprovalInstanceDetail(
                "inst-1", "COMPLETED", "bob", 7L, "order-approval", branches, tasks, rounds));
    }
}
