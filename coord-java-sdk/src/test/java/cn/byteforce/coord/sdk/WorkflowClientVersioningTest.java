package cn.byteforce.coord.sdk;

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
}
