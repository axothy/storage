import org.jreleaser.model.Active

plugins {
    java
    id("maven-publish")
    id("org.jreleaser") version "1.17.0"
}

group = "ru.axothy"
version = "1.1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.named("jreleaserFullRelease") {
    dependsOn(":storage-api:publish", ":storage-core:publish")
}

jreleaser {
    release {
        github {
            skipRelease = true
            skipTag = true
        }
    }
    signing {
        active = Active.ALWAYS
        armored = true
        verify = true
    }
    project {
        inceptionYear = "2025"
        author("@axothy")
    }
    deploy {
        maven {
            mavenCentral.create("sonatype") {
                active = Active.ALWAYS
                url = "https://central.sonatype.com/api/v1/publisher"
                retryDelay = 60
                stagingRepository(layout.buildDirectory.dir("staging-deploy").get().toString())
                setAuthorization("Basic")
            }
        }
    }
}
