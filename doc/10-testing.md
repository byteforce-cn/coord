# 单元测试 & 集成测试接入指南

本文介绍如何在微服务的单元测试 / 集成测试中接入 coord，
重点覆盖 **dev 模式 + 固定 root token** 方案，使测试配置可硬编码、无需每次解析日志。

---

## 一、核心思路

`coord-server dev` 模式支持 `COORD_DEV_ROOT_TOKEN` 环境变量：

- **首次启动**：自动初始化（1-of-1 Shamir）、嵌入 token、立即 unseal。
- **后续重启**：读 `<data_dir>/dev-unseal.share` 自动 unseal，token 值不变。
- `serve` 模式下此参数被忽略，不影响生产。

因此只需在测试容器中设置该环境变量，测试代码即可直接使用硬编码 token，无需任何握手初始化逻辑。

---

## 二、Testcontainers（Java）

### 依赖

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.20.6</version>
    <scope>test</scope>
</dependency>
```

### 测试基类

```java
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public abstract class CoordIntegrationBase {

    static final String COORD_IMAGE = "nexus.byteforce.cn/image-private/coord:0.1.9";
    static final int    GRPC_PORT   = 9090;
    static final int    HTTP_PORT   = 8080;
    static final String ROOT_TOKEN  = "s.integration-test-root";

    @SuppressWarnings("resource")
    protected static GenericContainer<?> startCoord() {
        GenericContainer<?> coord = new GenericContainer<>(
                DockerImageName.parse(COORD_IMAGE))
            .withCommand("dev")
            .withEnv("COORD_DEV_ROOT_TOKEN", ROOT_TOKEN)
            .withEnv("COORD_GRPC_ADDR", "0.0.0.0:" + GRPC_PORT)
            .withEnv("COORD_HTTP_ADDR", "0.0.0.0:" + HTTP_PORT)
            .withExposedPorts(GRPC_PORT, HTTP_PORT)
            .waitingFor(new HttpWaitStrategy()
                .forPath("/healthz")
                .forPort(HTTP_PORT)
                .withStartupTimeout(java.time.Duration.ofSeconds(60)));
        coord.start();
        return coord;
    }

    protected static String grpcEndpoint(GenericContainer<?> coord) {
        return coord.getHost() + ":" + coord.getMappedPort(GRPC_PORT);
    }
}
```

### 在 Spring Boot 测试中使用

```java
@SpringBootTest
@Testcontainers
class TransitEncryptionTest extends CoordIntegrationBase {

    @Container
    static GenericContainer<?> coord = startCoord();

    private TransitServiceGrpc.TransitServiceBlockingStub transitStub;

    @BeforeEach
    void setup() {
        ManagedChannel channel = ManagedChannelBuilder
            .forTarget(grpcEndpoint(coord))
            .usePlaintext()
            .build();
        // 携带固定 root_token
        Metadata meta = new Metadata();
        meta.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                 "Bearer " + ROOT_TOKEN);
        transitStub = TransitServiceGrpc.newBlockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
    }

    @Test
    void encrypt_and_decrypt_roundtrip() {
        // 创建密钥
        transitStub.createKey(CreateKeyRequest.newBuilder()
            .setKeyName("test-key").build());

        // 加密
        String ciphertext = transitStub.encrypt(EncryptRequest.newBuilder()
            .setKeyName("test-key")
            .setPlaintext("hello-coord")
            .build()).getCiphertext();

        // 解密
        String plaintext = transitStub.decrypt(DecryptRequest.newBuilder()
            .setKeyName("test-key")
            .setCiphertext(ciphertext)
            .build()).getPlaintext();

        assertThat(plaintext).isEqualTo("hello-coord");
    }
}
```

---

## 三、Testcontainers（Rust）

```toml
[dev-dependencies]
testcontainers = "0.23"
testcontainers-modules = "0.11"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tonic = "0.12"
```

```rust
use testcontainers::{core::{IntoContainerPort, WaitFor}, runners::AsyncRunner, GenericImage};

const COORD_IMAGE: &str = "nexus.byteforce.cn/image-private/coord";
const COORD_VERSION: &str = "0.1.9";
const ROOT_TOKEN: &str = "s.rust-test-root";

async fn start_coord() -> (impl Drop, String) {
    let container = GenericImage::new(COORD_IMAGE, COORD_VERSION)
        .with_exposed_port(9090_u16.tcp())
        .with_exposed_port(8080_u16.tcp())
        .with_env_var("COORD_DEV_ROOT_TOKEN", ROOT_TOKEN)
        .with_env_var("COORD_GRPC_ADDR", "0.0.0.0:9090")
        .with_env_var("COORD_HTTP_ADDR", "0.0.0.0:8080")
        .with_cmd(vec!["dev"])
        .with_wait_for(WaitFor::http(
            "/healthz".to_string(),
            8080,
            reqwest::StatusCode::OK,
        ))
        .start()
        .await
        .expect("failed to start coord container");

    let grpc_port = container.get_host_port_ipv4(9090).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{}", grpc_port);
    (container, endpoint)
}

#[tokio::test]
async fn test_coord_transit() {
    let (_c, endpoint) = start_coord().await;
    // 使用固定 token 建立 channel ...
    let _ = ROOT_TOKEN; // 在 metadata 中传入
}
```

---

## 四、Docker Compose 测试环境

在 `docker-compose.test.yml` 中固定 root token：

```yaml
services:
  coord-test:
    image: nexus.byteforce.cn/image-private/coord:0.1.9
    command: ["dev"]
    ports:
      - "9090:9090"
      - "8080:8080"
    environment:
      COORD_DEV_ROOT_TOKEN: "s.ci-test-root"
      COORD_GRPC_ADDR: "0.0.0.0:9090"
      COORD_HTTP_ADDR: "0.0.0.0:8080"
      COORD_DATA_DIR: "/data"
    volumes:
      - coord-test-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/healthz"]
      interval: 3s
      timeout: 3s
      retries: 20
      start_period: 10s

volumes:
  coord-test-data:
```

CI 脚本：

```bash
docker compose -f docker-compose.test.yml up -d
docker compose -f docker-compose.test.yml wait coord-test
mvn test -DCOORD_ENDPOINT=http://127.0.0.1:9090 -DCOORD_TOKEN=s.ci-test-root
docker compose -f docker-compose.test.yml down -v
```

---

## 五、本地二进制测试

不需要 Docker 时，直接启动本地二进制：

```bash
COORD_DEV_ROOT_TOKEN=s.local-test \
  cargo run -p coord-server -- dev \
  --data-dir /tmp/coord-test \
  --grpc-addr 127.0.0.1:19090 \
  --http-addr 127.0.0.1:19091 &

# 等待就绪
until curl -sf http://127.0.0.1:19091/healthz; do sleep 0.5; done

# 运行测试
cargo test --test my_integration_test -- \
  --coord-endpoint http://127.0.0.1:19090 \
  --coord-token s.local-test
```

---

## 六、注意事项

| 事项 | 说明 |
|------|------|
| token TTL | dev 模式嵌入的 root token TTL 为 1 年，CI 测试无需续期 |
| 数据隔离 | 每个测试套件应使用独立的 `COORD_DATA_DIR`，避免测试状态互相污染 |
| 容器复用 | 如果多个测试类共享同一容器（JUnit 5 `@Container` static），先 `operator seal-status` 检查状态 |
| 不要在生产使用 | `COORD_DEV_ROOT_TOKEN` 是明文配置，仅限测试和 dev 容器 |
| 安全域重置 | 清除 `<data_dir>` 或重建容器（不挂载 volume）即可重置安全域 |
