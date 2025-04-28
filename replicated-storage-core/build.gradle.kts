plugins {
    id("java")
}

group = "ru.axothy"
version = "1.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":replicated-storage-common"))

    implementation(project(":storage-api"))
    implementation(project(":storage-core"))

    implementation("org.apache.ratis:ratis-grpc:3.1.3")
    implementation("org.apache.ratis:ratis-server:3.1.3")
    implementation("org.apache.ratis:ratis-client:3.1.3")

    implementation("com.google.protobuf:protobuf-java:3.25.7")
    implementation("io.grpc:grpc-netty-shaded:1.72.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}