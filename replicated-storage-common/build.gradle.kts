plugins {
    id("java")
    id("com.google.protobuf") version "0.9.5"
}

group = "ru.axothy"
version = "1.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":storage-api"))
    implementation("com.google.protobuf:protobuf-java:3.25.7")  // или актуальную

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.0"
    }
    generatedFilesBaseDir = "$projectDir/src/generated"
}

sourceSets["main"].java.srcDir("$projectDir/src/generated/main/java")

tasks.test {
    useJUnitPlatform()
}