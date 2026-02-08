FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace

COPY gradle gradle
COPY gradlew gradlew
COPY settings.gradle.kts build.gradle.kts ./
COPY api/build.gradle.kts api/build.gradle.kts
COPY api/src api/src

RUN chmod +x gradlew \
  && ./gradlew :api:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /workspace/api/build/libs/*.jar /app/app.jar

EXPOSE 8080

ENTRYPOINT ["java","-jar","/app/app.jar"]
