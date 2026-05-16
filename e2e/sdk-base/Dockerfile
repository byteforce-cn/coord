# Build context: project root (coord/)
# Produces: coord-sdk-base:local  — Maven repo with coord-sdk-java pulled from Nexus
#
# coord-sdk-java is now maintained in private/java-libs and published to
# https://nexus.byteforce.cn/repository/maven-releases/
# Build secret: NEXUS_USER / NEXUS_PASSWORD (passed via env at build time)
FROM docker.1ms.run/maven:3.9.11-eclipse-temurin-21

# Write Maven settings with Nexus credentials (injected as build arg)
ARG NEXUS_USER
ARG NEXUS_PASSWORD
RUN mkdir -p /root/.m2 && printf '\
<settings>\
  <mirrors>\
    <mirror>\
      <id>aliyun-central</id>\
      <mirrorOf>central</mirrorOf>\
      <url>https://maven.aliyun.com/repository/central</url>\
    </mirror>\
  </mirrors>\
  <servers>\
    <server>\
      <id>byteforce-releases</id>\
      <username>%s</username>\
      <password>%s</password>\
    </server>\
  </servers>\
  <profiles>\
    <profile>\
      <id>byteforce</id>\
      <repositories>\
        <repository>\
          <id>byteforce-releases</id>\
          <url>https://nexus.byteforce.cn/repository/maven-releases/</url>\
          <releases><enabled>true</enabled></releases>\
          <snapshots><enabled>false</enabled></snapshots>\
        </repository>\
      </repositories>\
    </profile>\
  </profiles>\
  <activeProfiles><activeProfile>byteforce</activeProfile></activeProfiles>\
</settings>\n' "$NEXUS_USER" "$NEXUS_PASSWORD" > /root/.m2/settings.xml

# Pull coord-sdk-java from Nexus into the local Maven repo
ARG COORD_SDK_VERSION=0.2.0
RUN mvn dependency:get \
      -Dartifact=cn.byteforce:coord-sdk-java:${COORD_SDK_VERSION}:jar \
      -q
