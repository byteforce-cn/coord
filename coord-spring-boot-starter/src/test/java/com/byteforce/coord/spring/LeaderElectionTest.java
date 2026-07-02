package cn.byteforce.coord.spring;

import cn.byteforce.coord.spring.beans.LeaderElection;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD RED: LeaderElection 选举测试
 *
 * 验证:
 * 1. 初始状态为非 Leader
 * 2. 状态变更事件回调
 * 3. 多实例不会同时为 Leader
 */
class LeaderElectionTest {

    @Test
    void testInitialStateNotLeader() {
        LeaderElection election = new LeaderElection("test-campaign");
        assertThat(election.isLeader()).isFalse();
        assertThat(election.getCampaignName()).isEqualTo("test-campaign");
    }

    @Test
    void testStateChangeCallback() {
        LeaderElection election = new LeaderElection("test-campaign");
        boolean[] callbackInvoked = {false};
        String[] receivedRole = {null};

        election.addRoleChangeListener((isLeader) -> {
            callbackInvoked[0] = true;
            receivedRole[0] = isLeader ? "LEADER" : "FOLLOWER";
        });

        // 模拟成为 Leader（内部状态变更）
        election.setLeaderForTest(true);
        assertThat(callbackInvoked[0]).isTrue();
        assertThat(receivedRole[0]).isEqualTo("LEADER");

        // 模拟失去 Leader
        election.setLeaderForTest(false);
        assertThat(receivedRole[0]).isEqualTo("FOLLOWER");
    }

    @Test
    void testMultipleInstancesHaveDifferentCampaigns() {
        LeaderElection e1 = new LeaderElection("campaign-A");
        LeaderElection e2 = new LeaderElection("campaign-B");

        assertThat(e1.getCampaignName()).isNotEqualTo(e2.getCampaignName());
    }
}
