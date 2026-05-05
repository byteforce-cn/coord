# Build context: project root (coord/)
# Requires: coord-sdk-base:local (built by `make build`)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# ── 1. Seed Maven repo from pre-built SDK base image ─────────
# Cached until coord-sdk-base:local digest changes
COPY --from=coord-sdk-base:local /root/.m2 /root/.m2

# ── 2. Download service deps (cached until any pom changes) ───
COPY e2e/mock-services/pom.xml mock-services/pom.xml
COPY e2e/mock-services/order-service/pom.xml mock-services/order-service/pom.xml
COPY e2e/mock-services/pay-service/pom.xml mock-services/pay-service/pom.xml
COPY e2e/mock-services/inventory-service/pom.xml mock-services/inventory-service/pom.xml
RUN mvn -f mock-services/pom.xml -pl order-service dependency:go-offline -q

# ── 3. Build (cached until source changes) ────────────────────
COPY e2e/mock-services/order-service/src mock-services/order-service/src
RUN mvn -f mock-services/pom.xml -pl order-service package -DskipTests -q

# ── Runtime ───────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /build/mock-services/order-service/target/order-service-*.jar app.jar
EXPOSE 18080
ENTRYPOINT ["java", "-jar", "app.jar"]
