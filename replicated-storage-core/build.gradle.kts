plugins {
    id("java")
    id("com.github.johnrengelman.shadow") version "8.1.1"
    application
}

group = "ru.axothy"
version = "1.1.0"

repositories {
    mavenCentral()
}

application {
    mainClass.set("ru.axothy.LsmRaftServer")
}

dependencies {
    implementation(project(":replicated-storage-common"))

    implementation(project(":storage-api"))
    implementation(project(":storage-core"))

    implementation("org.apache.ratis:ratis-grpc:3.1.3")
    implementation("org.apache.ratis:ratis-server:3.1.3")
    implementation("org.apache.ratis:ratis-client:3.1.3")
    implementation("org.apache.ratis:ratis-metrics-default:3.1.3")

    implementation("com.google.protobuf:protobuf-java:3.25.7")
    implementation("io.grpc:grpc-netty-shaded:1.72.0")

    implementation("io.javalin:javalin-bundle:6.6.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.19.0")

    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

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

tasks.shadowJar {
    archiveBaseName.set("lsmraft")
    archiveClassifier.set("")
    archiveVersion.set("")

    mergeServiceFiles()
}