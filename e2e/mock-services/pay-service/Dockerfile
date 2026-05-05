# Build context: project root (coord/)
# Requires: coord-sdk-base:local (built by `make build`)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

COPY --from=coord-sdk-base:local /root/.m2 /root/.m2

COPY e2e/mock-services/pom.xml mock-services/pom.xml
COPY e2e/mock-services/order-service/pom.xml mock-services/order-service/pom.xml
COPY e2e/mock-services/pay-service/pom.xml mock-services/pay-service/pom.xml
COPY e2e/mock-services/inventory-service/pom.xml mock-services/inventory-service/pom.xml
RUN mvn -f mock-services/pom.xml -pl pay-service dependency:go-offline -q

COPY e2e/mock-services/pay-service/src mock-services/pay-service/src
RUN mvn -f mock-services/pom.xml -pl pay-service package -DskipTests -q

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /build/mock-services/pay-service/target/pay-service-*.jar app.jar
EXPOSE 18081
ENTRYPOINT ["java", "-jar", "app.jar"]
