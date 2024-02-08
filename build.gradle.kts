import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.tasks.bundling.BootBuildImage

plugins {
	id("org.springframework.boot") version "3.2.1"
	id("io.spring.dependency-management") version "1.1.3"
	kotlin("jvm") version "1.9.10"
	kotlin("plugin.spring") version "1.9.10"
}

group = "com.ailegorreta"
version = "2.0.0"
description = "Bup-bulk-service receives kafka nessage from ingestor and insert in a bulk manner" +
              "Persons, Companies, Address, etc in the BUP database.."
java {
	sourceCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenLocal()
	mavenCentral()

    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/" +
                        project.findProperty("registryPackageUrl") as String? ?:
                        System.getenv("URL_PACKAGE") ?:
                        "rlegorreta/ailegorreta-kit")
        credentials {
            username = project.findProperty("registryUsername") as String? ?:
                        System.getenv("USERNAME") ?:
                        "rlegorreta"
            password = project.findProperty("registryToken") as String? ?: System.getenv("TOKEN")
        }
    }
}

extra["springCloudVersion"] = "2023.0.0"
extra["testcontainersVersion"] = "1.17.3"
extra["otelVersion"] = "1.26.0"
extra["ailegorreta-kit-version"] = "2.0.0"
extra["coroutines-version"] = "1.7.3"

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-config")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-webflux")

    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-client")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")

    implementation("org.springframework.cloud:spring-cloud-starter-netflix-eureka-client") {
        exclude(group = "org.springframework.cloud", module = "spring-cloud-starter-ribbon")
        exclude(group = "com.netflix.ribbon", module = "ribbon-eureka")
    }
    // ^ This library work just for docker container. Kubernetes ignores it (setting eureka.client.registerWithEureka
    // property to false

    implementation("org.springframework.cloud:spring-cloud-stream")
    implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly("io.opentelemetry.javaagent:opentelemetry-javaagent:${property("otelVersion")}")

    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${property("coroutines-version")}")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:${property("coroutines-version")}")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")

    implementation("com.ailegorreta:ailegorreta-kit-commons-utils:${property("ailegorreta-kit-version")}")
    implementation("com.ailegorreta:ailegorreta-kit-resource-server-security:${property("ailegorreta-kit-version")}")
    implementation("com.ailegorreta:ailegorreta-kit-commons-event:${property("ailegorreta-kit-version")}")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux")
    testImplementation("org.springframework.security:spring-security-test")
    testImplementation("org.springframework.cloud:spring-cloud-stream-test-binder")
    // testImplementation("io.projectreactor:reactor-test")                // this is for web-flux testing
    testImplementation("com.squareup.okhttp3:mockwebserver")
    testImplementation("io.mockk:mockk:1.9.3")

    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
}

dependencyManagement {
	imports {
		mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
        mavenBom("org.testcontainers:testcontainers-bom:${property("testcontainersVersion")}")
	}
}

tasks.named<BootBuildImage>("bootBuildImage") {
    environment.set(environment.get() + mapOf("BP_JVM_VERSION" to "17.*"))
    imageName.set("ailegorreta/${project.name}")
    docker {
        publishRegistry {
            username.set(project.findProperty("registryUsername").toString())
            password.set(project.findProperty("registryToken").toString())
            url.set(project.findProperty("registryUrl").toString())
        }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

configure<SourceSetContainer> {
    named("main") {
        java.srcDir("src/main/kotlin")
    }
}
