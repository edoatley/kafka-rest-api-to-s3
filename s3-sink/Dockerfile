FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace

COPY gradle gradle
COPY gradlew gradlew
COPY settings.gradle.kts build.gradle.kts ./
COPY s3-sink/build.gradle.kts s3-sink/build.gradle.kts
COPY s3-sink/src s3-sink/src

RUN chmod +x gradlew \
  && ./gradlew :s3-sink:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /workspace/s3-sink/build/libs/*.jar /app/app.jar

ENTRYPOINT ["java","-jar","/app/app.jar"]
