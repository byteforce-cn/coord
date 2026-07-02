package cn.byteforce.coord.spring.beans;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Leader 选举 Bean
 *
 * 封装 Coord Agent 的 LeaderElectionService，提供角色变更事件回调。
 *
 * 使用示例:
 * <pre>{@code
 * @Autowired
 * private LeaderElection leaderElection;
 *
 * @PostConstruct
 * void init() {
 *     leaderElection.addRoleChangeListener(isLeader -> {
 *         if (isLeader) startScheduler();
 *         else stopScheduler();
 *     });
 * }
 * }</pre>
 */
public class LeaderElection {

    private final String campaignName;
    private volatile boolean isLeader = false;
    private final List<Consumer<Boolean>> listeners = new CopyOnWriteArrayList<>();

    public LeaderElection(String campaignName) {
        this.campaignName = campaignName;
    }

    public String getCampaignName() {
        return campaignName;
    }

    public boolean isLeader() {
        return isLeader;
    }

    /**
     * 添加角色变更监听器
     *
     * @param listener 回调函数，参数 true 表示成为 Leader，false 表示失去 Leader
     */
    public void addRoleChangeListener(Consumer<Boolean> listener) {
        listeners.add(listener);
    }

    /**
     * 移除角色变更监听器
     */
    public void removeRoleChangeListener(Consumer<Boolean> listener) {
        listeners.remove(listener);
    }

    /**
     * 仅供测试使用: 模拟 Leader 状态变更
     */
    public void setLeaderForTest(boolean leader) {
        if (this.isLeader != leader) {
            this.isLeader = leader;
            notifyListeners();
        }
    }

    private void notifyListeners() {
        boolean current = isLeader;
        for (Consumer<Boolean> listener : listeners) {
            try {
                listener.accept(current);
            } catch (Exception e) {
                // 防止单个监听器异常影响其他监听器
            }
        }
    }
}
