plugins {
    java
    id("org.springframework.boot") version "4.0.2"
    id("io.spring.dependency-management") version "1.1.7"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

springBoot {
    mainClass.set("com.example.kafkarestapi.KafkaRestApiApplication")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("io.micrometer:micrometer-tracing-bridge-otel")
    implementation("io.micrometer:micrometer-registry-otlp")
    implementation("org.apache.avro:avro:1.12.1")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.20.6"))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.register<org.springframework.boot.gradle.tasks.run.BootRun>("localBootRun") {
    group = "application"
    description = "Run the app with the local Spring profile."
    mainClass.set("com.example.kafkarestapi.KafkaRestApiApplication")
    classpath = sourceSets["main"].runtimeClasspath
    systemProperty("spring.profiles.active", "local")
    environment("SPRING_PROFILES_ACTIVE", "local")
}

sourceSets {
    main {
        java.srcDir(layout.buildDirectory.dir("generated-main-avro-java"))
    }
}

avro {
    setCreateSetters(false)
    setFieldVisibility("PRIVATE")
    setStringType("String")
}
