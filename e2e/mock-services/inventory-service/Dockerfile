# Build context: project root (coord/)
# Requires: coord-sdk-base:local (built by `make build`)
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build

COPY --from=coord-sdk-base:local /root/.m2 /root/.m2

COPY e2e/mock-services/pom.xml mock-services/pom.xml
COPY e2e/mock-services/order-service/pom.xml mock-services/order-service/pom.xml
COPY e2e/mock-services/pay-service/pom.xml mock-services/pay-service/pom.xml
COPY e2e/mock-services/inventory-service/pom.xml mock-services/inventory-service/pom.xml
RUN mvn -f mock-services/pom.xml -pl inventory-service dependency:go-offline -q

COPY e2e/mock-services/inventory-service/src mock-services/inventory-service/src
RUN mvn -f mock-services/pom.xml -pl inventory-service package -DskipTests -q

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/mock-services/inventory-service/target/inventory-service-*.jar app.jar
EXPOSE 18082
ENTRYPOINT ["java", "-jar", "app.jar"]
