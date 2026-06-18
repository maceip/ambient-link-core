plugins {
    id("com.android.library") version "8.7.0"
    id("org.jetbrains.kotlin.android") version "2.0.21"
    `maven-publish`
}

group = "com.ambientlink"
version = "0.1.0"

android {
    namespace = "com.ambientlink.core"
    compileSdk = 35

    defaultConfig {
        // Floor across the consumers: glasses app (29) and Wear (30).
        minSdk = 29
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }

    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }
}

dependencies {
    // Exposed as `api` so consumers get StateFlow/coroutines transitively.
    api("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")
}

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("release") {
                from(components["release"])
                groupId = "com.ambientlink"
                artifactId = "core-android"
                version = "0.1.0"
            }
        }
    }
}
