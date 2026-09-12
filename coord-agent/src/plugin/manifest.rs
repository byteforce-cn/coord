// coord-agent: 插件 manifest 规格
//
// 一个插件 = manifest（元数据 + 资源限制 + 能力声明）+ JS 脚本或 wasm 模块。
// manifest 可来自 `[plugins.entries]`（配置）或插件目录扫描（后续阶段）。

use serde::{Deserialize, Serialize};

// ──── 运行时 / 信任等级 ────

/// 插件运行时。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRuntime {
    /// rquickjs 内嵌 JS/TS 编排插件（第一方主路径）
    Js,
    /// wasmtime 组件模型 wasm 模块（强沙箱）
    Wasm,
    /// 原生 Rust 服务（经 `NativePluginAdapter` 包装的既有 BaseService）
    Native,
}

impl PluginRuntime {
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginRuntime::Js => "js",
            PluginRuntime::Wasm => "wasm",
            PluginRuntime::Native => "native",
        }
    }
}

/// 插件信任等级（决定强制沙箱手段）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginTrust {
    /// 第一方：轻沙箱（内存上限 + 执行超时 + SDK 白名单）
    FirstParty,
    /// 第三方：强沙箱（fuel + epoch + 内存上限 + 无 WASI + capability 白名单）
    ThirdParty,
}

impl PluginTrust {
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginTrust::FirstParty => "first_party",
            PluginTrust::ThirdParty => "third_party",
        }
    }
}

/// 插件来源（预留：目录 / kv / 对象存储桶）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PluginSource {
    /// 本地文件系统路径（相对 `[plugins].dir`）
    Path(String),
    /// 协调 KV 键（预留）
    Kv(String),
    /// 对象存储桶（预留）
    StorageBucket(String),
}

impl Default for PluginSource {
    fn default() -> Self {
        PluginSource::Path(String::new())
    }
}

// ──── 能力声明 ────

/// 插件声明的能力 + scope。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginCapability {
    /// capability ID（如 "data:kv:read"）
    pub id: String,
    /// scope 限制（空 = 无限制；如 "/app/counter/"）
    #[serde(default)]
    pub scope: String,
}

// ──── 资源限制 ────

fn default_max_memory_mb() -> u32 {
    64
}
fn default_max_exec_ms() -> u64 {
    30_000
}
fn default_max_objects() -> u32 {
    64
}

/// 单插件资源限制（可覆盖 `[plugins.default_limits]`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginLimits {
    /// 内存上限（MiB）
    #[serde(default = "default_max_memory_mb")]
    pub max_memory_mb: u32,
    /// 单次调用执行时间上限（毫秒）
    #[serde(default = "default_max_exec_ms")]
    pub max_exec_ms: u64,
    /// 单插件可并发的协调调用句柄上限
    #[serde(default = "default_max_objects")]
    pub max_objects: u32,
}

impl Default for PluginLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: default_max_memory_mb(),
            max_exec_ms: default_max_exec_ms(),
            max_objects: default_max_objects(),
        }
    }
}

// ──── Manifest ────

/// 插件 manifest。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginManifest {
    /// 唯一名称（`[plugins.entries]` 与 SPI 的键）
    pub name: String,
    /// 语义化版本
    #[serde(default)]
    pub version: String,
    /// 运行时
    pub runtime: PluginRuntime,
    /// 信任等级
    pub trust: PluginTrust,
    /// 入口（脚本/模块路径，相对 `[plugins].dir`）
    #[serde(default)]
    pub entry: String,
    /// 能力声明
    #[serde(default)]
    pub capabilities: Vec<PluginCapability>,
    /// 资源限制（缺省用引擎默认值）
    #[serde(default)]
    pub limits: PluginLimits,
    /// 是否启用拦截点
    #[serde(default)]
    pub hooks: bool,
    /// 来源（缺省 `entry` 作为路径）
    #[serde(default)]
    pub source: PluginSource,
}

impl PluginManifest {
    /// 校验 manifest 合法性（加载前置门禁）。
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("plugin name must not be empty".into());
        }
        if self.name.len() > 128 {
            return Err("plugin name too long (max 128)".into());
        }
        if self
            .name
            .chars()
            .any(|c| c.is_whitespace() || c == '/' || c == '\\')
        {
            return Err("plugin name must not contain whitespace or path separators".into());
        }
        if self.entry.trim().is_empty() {
            return Err(format!("plugin '{}' entry must not be empty", self.name));
        }
        if self.entry.contains("..") {
            return Err(format!(
                "plugin '{}' entry must not contain '..' (path traversal)",
                self.name
            ));
        }
        if self.limits.max_memory_mb == 0 {
            return Err(format!("plugin '{}' max_memory_mb must be > 0", self.name));
        }
        if self.limits.max_exec_ms == 0 {
            return Err(format!("plugin '{}' max_exec_ms must be > 0", self.name));
        }
        if self.limits.max_objects == 0 {
            return Err(format!("plugin '{}' max_objects must be > 0", self.name));
        }
        for cap in &self.capabilities {
            if cap.id.trim().is_empty() {
                return Err(format!(
                    "plugin '{}' declares an empty capability id",
                    self.name
                ));
            }
        }
        Ok(())
    }

    /// 解出实际入口路径（`source` 显式路径优先，否则用 `entry`）。
    pub fn resolved_entry(&self) -> &str {
        match &self.source {
            PluginSource::Path(p) if !p.is_empty() => p,
            _ => &self.entry,
        }
    }
}

// ──── 引擎配置 ────

fn default_plugins_dir() -> String {
    "/var/lib/coord-agent/plugins".into()
}
fn default_true() -> bool {
    true
}

/// `[plugins]` 插件引擎配置。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginEngineConfig {
    /// 总开关（false = 插件引擎完全关闭，零开销）
    #[serde(default)]
    pub enabled: bool,
    /// 插件根目录（`entry` / `source` 相对此目录解析）
    #[serde(default = "default_plugins_dir")]
    pub dir: String,
    /// 网关层 + 调用面拦截点总开关
    #[serde(default = "default_true")]
    pub hooks_enabled: bool,
    /// 每插件默认资源限制
    #[serde(default)]
    pub default_limits: PluginLimits,
    /// 注入所有插件的只读配置（插件经 `coord.env(key)` 读取）
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// 静态配置的插件条目
    #[serde(default)]
    pub entries: Vec<PluginManifest>,
}

impl Default for PluginEngineConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: default_plugins_dir(),
            hooks_enabled: true,
            default_limits: PluginLimits::default(),
            env: std::collections::BTreeMap::new(),
            entries: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(name: &str) -> PluginManifest {
        PluginManifest {
            name: name.into(),
            version: "1.0.0".into(),
            runtime: PluginRuntime::Js,
            trust: PluginTrust::FirstParty,
            entry: "index.js".into(),
            capabilities: vec![],
            limits: PluginLimits::default(),
            hooks: false,
            source: PluginSource::default(),
        }
    }

    #[test]
    fn validate_accepts_well_formed_manifest() {
        assert!(manifest("dist-counter").validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_name_and_entry() {
        let mut m = manifest("x");
        m.name = "  ".into();
        assert!(m.validate().is_err());
        let mut m = manifest("x");
        m.entry = "".into();
        assert!(m.validate().is_err());
    }

    #[test]
    fn validate_rejects_path_traversal() {
        let mut m = manifest("x");
        m.entry = "../evil.js".into();
        assert!(m.validate().is_err());
    }

    #[test]
    fn resolved_entry_prefers_explicit_source_path() {
        let mut m = manifest("x");
        m.source = PluginSource::Path("custom/path.js".into());
        assert_eq!(m.resolved_entry(), "custom/path.js");
        m.source = PluginSource::default();
        assert_eq!(m.resolved_entry(), "index.js");
    }

    #[test]
    fn config_defaults_and_toml_roundtrip() {
        let cfg = PluginEngineConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.hooks_enabled);

        let toml_src = r#"
enabled = true
dir = "/tmp/plugins"
[[entries]]
name = "dist-counter"
version = "1.0.0"
runtime = "js"
trust = "first_party"
entry = "dist-counter/index.js"
hooks = true
capabilities = [{ id = "data:kv:read", scope = "/app/counter/" }]
"#;
        let parsed: PluginEngineConfig = toml::from_str(toml_src).unwrap();
        assert!(parsed.enabled);
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].runtime, PluginRuntime::Js);
        assert_eq!(parsed.entries[0].capabilities[0].id, "data:kv:read");
        // 缺省 limits 填 default
        assert_eq!(parsed.entries[0].limits.max_memory_mb, 64);
    }
}
