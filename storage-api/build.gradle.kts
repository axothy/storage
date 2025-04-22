plugins {
    java
    id("maven-publish")
}

group = "ru.axothy"
version = "1.1"

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

java {
    withJavadocJar()
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("release") {
            from(components["java"])

            groupId = "ru.axothy"
            artifactId = "storage-api"

            pom {
                name.set("LSM Storage API")
                description.set("DAO API")
                url.set("https://github.com/axothy/storage")
                issueManagement {
                    url.set("https://github.com/axothy/storage/issues")
                }

                scm {
                    url.set("https://github.com/axothy/storage")
                    connection.set("scm:git://github.com/axothy/storage.git")
                    developerConnection.set("scm:git://github.com/axothy/storage.git")
                }

                licenses {
                    license {
                        name.set("The Apache Software License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }

                developers {
                    developer {
                        id.set("axothy")
                        name.set("Alexandr Chebotin")
                        email.set("globaltouareg@gmail.com")
                        url.set("https://github.com/axothy")
                    }
                }
            }
        }
    }

    repositories {
        maven {
            setUrl(rootProject.layout.buildDirectory.dir("staging-deploy").get().asFile.absolutePath)
        }
    }
}
