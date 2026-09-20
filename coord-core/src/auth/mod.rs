// Auth module — CCT v3 token encoding/decoding, scope validation, capability model

pub mod cct;
pub mod trie;

/// 引导管理员角色名。
///
/// **它是"全能力"的语义常量，必须被每一个授权面同等对待。**
///
/// 存在的原因（jepsen F-32）：server 侧的 `check_capability` 对 `root` 直接放行
/// （引导管理员，全能力），而 agent 侧只按 RoleCache 里的**显式能力集**判定 ——
/// root 的角色记录里本来就没有逐项能力（全靠 server 的旁路兜着），于是一张 root
/// 的 CCT 在 agent 侧等于"没有任何能力"：
///
/// ```text
/// UNAUTHENTICATED: role(s) ["root"] do not have capability 'data:kv:read'
/// ```
///
/// 也就是说 **auth 开启后，agent 作为应用入口的整条路径对 root/管理员不可用**，
/// 而这一点在任何进程内测试里都看不到（进程内测试直接调 server，绕过了 agent）。
///
/// **修法选了"两侧共用同一个常量 + agent 侧同口径旁路"**，而不是"把 root 的
/// 全能力物化进角色记录"：后者引入新的漂移面（以后新增的能力不会自动进那条记录，
/// 于是 root 会缺能力，且只在运行期可见）—— 与本仓反复出现过的"自述式缺陷"同族。
pub const ROOT_ROLE: &str = "root";

/// 给定的角色集合里是否含引导管理员。
///
/// 仅供**授权判定**使用（server 与 agent 的检查点都必须先问这一个函数），
/// 不要拿它做审计/展示用途的逻辑分支。
pub fn is_root(roles: &[String]) -> bool {
    roles.iter().any(|r| r == ROOT_ROLE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_role_detected() {
        assert!(is_root(&["root".to_string()]));
        assert!(is_root(&["reader".to_string(), "root".to_string()]));
        assert!(!is_root(&["reader".to_string()]));
        assert!(!is_root(&[]));
        // 前缀/大小写都不是 root：契约是逐字相等
        assert!(!is_root(&["root-admin".to_string()]));
        assert!(!is_root(&["Root".to_string()]));
    }
}
