plugins {
    id("buildlogic.scala-application-conventions")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

dependencies {
    compileOnly("org.apache.flink:flink-streaming-java:1.17.2")
    implementation("org.apache.flink:flink-connector-files:1.17.2")
    implementation("org.apache.flink:flink-connector-datagen:1.17.2")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_2.12:2.17.2")
}

application {
    // Define the main class for the application.
    mainClass = "stock.aggregator.StockAggregatorPipeline"
}

tasks.build {
    enabled = false
    dependsOn(tasks.shadowJar)
}

tasks.shadowJar {
    archiveClassifier = null
}
