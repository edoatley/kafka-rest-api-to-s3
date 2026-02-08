import org.gradle.api.plugins.JavaPluginExtension

plugins {
    base
}

subprojects {
    group = "com.example"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    plugins.withId("java") {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(21)
            }
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
