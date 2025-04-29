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

    implementation("org.apache.ratis:ratis-grpc:3.1.3")
    implementation("com.google.protobuf:protobuf-java:3.25.7")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.0"
    }
}

sourceSets["main"].java.srcDirs(
    "src/main/java"
)

tasks.test {
    useJUnitPlatform()
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("--enable-preview")
}

tasks.withType<Test>().configureEach {
    jvmArgs("--enable-preview")
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs("--enable-preview")
}