import java.io.ByteArrayOutputStream

plugins {
    `java-library`
    `maven-publish`
    signing
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
    }
}

version = getVersionFromGit()
group = "org.duckdb"
description = "JDBC library for DuckDB"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
    // TODO fix javadoc first
    // withJavadocJar()
    // TODO change in next PR
    // withSourcesJar()
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")
}

tasks {
    register<Sync>("syncSo") {
        from("$projectDir/../../build/release/tools/jdbc")
        include("libduckdb_java.so_*")
        into("$projectDir/src/main/resources")
        preserve {
            include("META-INF/*")
        }
    }

    processResources {
        dependsOn("syncSo")
    }

    named<Test>("test") {
        useJUnitPlatform()
        testLogging.showExceptions = true
    }

    javadoc {
        options.encoding = "UTF-8"
    }

    withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    withType<Test> {
        systemProperty("file.encoding", "UTF-8")
    }

    compileJava.configure {
        options.release.set(8)
        // this will be used to generate headers to compile cpp code first
        options.compilerArgs.addAll(listOf("-h", "${buildDir}/target/headers"))
    }
}

fun getVersionFromGit(): String {
    ByteArrayOutputStream().use {
        exec {
            workingDir = projectDir
            args = listOf("describe", "--tags", "--always", "--dirty=-SNAPSHOT")
            executable = "git"
            standardOutput = it
        }
        val version = it.toString().trim()
        val pattern = """(\d*)\.(\d*)\.(\d*)(-(\d*)-([a-zA-Z\d]*))?""".toRegex()
        val matcher = pattern.find(version) ?: throw IllegalStateException("Could not parse version $version")
        val major = matcher.groupValues[1].toInt()
        val minor = matcher.groupValues[2].toInt()
        val patch = matcher.groupValues[3].toInt()
        val snapshot = if (version.endsWith("SNAPSHOT")) "-SNAPSHOT" else ""
        return "${major}.${minor}.${patch}${snapshot}"
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "duckdb_jdbc"
            from(components["java"])
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("DuckDB JDBC Driver")
                description.set("A JDBC-Compliant driver for the DuckDB data management system")
                url.set("https://www.duckdb.org")
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://raw.githubusercontent.com/duckdb/duckdb/master/LICENSE")
                    }
                }
                developers {
                    developer {
                        id.set("mra")
                        name.set("Mark Raasveldt")
                        email.set("mark@duckdblabs.com")
                    }
                    developer {
                        id.set("hmu")
                        name.set("Hannes Muehleisen")
                        email.set("hannes@duckdblabs.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/duckdb/duckdb.git")
                    developerConnection.set("scm:git:ssh://github.com:duckdb/duckdb.git")
                    url.set("http://github.com/duckdb/duckdb/tree/master")
                }
            }
        }
    }
    repositories {
        maven {
            name = "ossrh"
            val releasesRepoUrl = "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
            val snapshotsRepoUrl = "https://s01.oss.sonatype.org/content/repositories/snapshots/"
            url = uri(
                when {
                    version.toString().endsWith("SNAPSHOT") -> snapshotsRepoUrl
                    else -> releasesRepoUrl
                }
            )
            credentials {
                username = findProperty("ossrhUsername")?.toString()
                password = findProperty("ossrhPassword")?.toString()
            }
        }
    }
}

signing {
    // only sign releases when publishing
    setRequired { !version.toString().endsWith("SNAPSHOT") && gradle.taskGraph.hasTask("publish") }
    sign(publishing.publications["mavenJava"])
}
