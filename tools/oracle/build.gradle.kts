plugins {
    java
    application
}

repositories { mavenCentral() }

// Keep this version equal to the `ref` in `crates/protocol/schemas/VERSION`.
// The oracle checks the codecs generated from those schemas, so it must use
// the Kafka release they came from. `docs/CONTRIBUTING.md` bumps both
// together.
val kafkaVersion = "4.3.0"

dependencies {
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    // The generated metadata records (`PartitionRecord` and others) and the
    // remote log metadata records. The oracle uses only their generated
    // message and JSON converter classes, which need nothing beyond
    // kafka-clients, so their own dependencies stay out.
    implementation("org.apache.kafka:kafka-metadata:$kafkaVersion") { isTransitive = false }
    implementation("org.apache.kafka:kafka-storage:$kafkaVersion") { isTransitive = false }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.2")
    // Compression codec libraries. The `compress` and `decompress` ops need
    // them at compile time.
    implementation("org.xerial.snappy:snappy-java:1.1.10.8")
    implementation("com.github.luben:zstd-jni:1.5.7-15")
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

application { mainClass.set("com.krabka.oracle.Oracle") }
