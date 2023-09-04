plugins {
    kotlin("jvm") version "1.9.0"
    application
}

group = "org.genspectrum"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.alibaba.fastjson2:fastjson2-kotlin:2.0.40")
    implementation("com.github.ajalt.clikt:clikt:4.2.0")
    implementation("com.github.luben:zstd-jni:1.5.5-5")
    implementation("org.apache.commons:commons-csv:1.10.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(8)
}

application {
    mainClass.set("MainKt")
}
