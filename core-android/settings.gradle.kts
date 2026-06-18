pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

// Single-module library build. Consumed by the vendor repos either as a Gradle
// composite build (`includeBuild("../ambient-link-core/core-android")`) or from a
// Maven repo after `./gradlew publishToMavenLocal`.
rootProject.name = "core-android"
