plugins {
    id("com.github.node-gradle.node") version "7.0.2"
}

node {
    version.set("20.11.1")
    download.set(true)
    npmWorkDir.set(file("${projectDir}"))
}

val npmBuild = tasks.register<com.github.gradle.node.npm.task.NpmTask>("npmBuild") {
    dependsOn(tasks.named("npmInstall"))
    args.set(listOf("run", "build"))
    workingDir.set(projectDir)
}

val copyFrontend = tasks.register<Copy>("copyFrontend") {
    dependsOn(npmBuild)
    from("$projectDir/dist")
    into("$rootDir/replicated-storage-core/src/main/resources/static")
}