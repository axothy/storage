plugins {
    id("com.github.node-gradle.node") version "7.0.2"
}

node {
    version.set("20.11.1")
    download.set(true)
    npmWorkDir.set(file("${projectDir}"))
}

tasks.register<Exec>("npmInstall") {
    group = "build"
    workingDir = projectDir
    commandLine("npm", "install")
}

tasks.register<Exec>("npmBuild") {
    group = "build"
    dependsOn("npmInstall")
    workingDir = projectDir
    commandLine("npm", "run", "build")
}

tasks.named("assemble").configure { dependsOn(":storage-ui:npmBuild") }

val copyToRatis = tasks.register<Copy>("copyFrontend") {
    dependsOn("npmBuild")
    from("$projectDir/dist")
    into("$rootDir/storage-ratis/src/main/resources/static")
}
tasks.named("npmBuild").configure { finalizedBy(copyToRatis) }
