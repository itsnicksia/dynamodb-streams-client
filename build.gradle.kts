plugins {
    kotlin("jvm") version "1.9.23"
}

group = "io.fasterthoughts"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("aws.sdk.kotlin:dynamodb:1.2.14")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(19)
}