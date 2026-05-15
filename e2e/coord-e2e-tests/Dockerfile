# Build context: project root (coord/)
# Reuse coord-sdk-base:local — 由 `make sdk-base` 提前构建，避免在此重复编译 SDK jar。

# Stage 1: Compile e2e tests (reuse Maven repo from sdk-base)
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build

# 复用 sdk-base 中已安装的 SDK jar 及 Maven 本地仓库，跳过 SDK 重复构建
COPY --from=coord-sdk-base:local /root/.m2 /root/.m2

COPY e2e/coord-e2e-tests/pom.xml e2e-tests/pom.xml
RUN mvn -f e2e-tests/pom.xml dependency:go-offline -q

COPY e2e/coord-e2e-tests/src e2e-tests/src
RUN mvn -f e2e-tests/pom.xml test-compile -q
# 预下载 surefire JUnit Platform provider（无副作用，明确传 -DskipTests）
RUN mvn -f e2e-tests/pom.xml dependency:resolve-plugins -q

# Stage 2: Runtime — run tests against live environment
FROM maven:3.9.11-eclipse-temurin-21
WORKDIR /app
COPY --from=build /build/e2e-tests /app
COPY --from=build /root/.m2 /root/.m2
CMD ["mvn", "test", "-o", "-Dmaven.test.failure.ignore=false"]
