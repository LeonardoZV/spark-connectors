package com.leonardozv.spark.connectors.aws.dynamodb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Paths;

@Testcontainers
class Spark3_3_0IntegrationTest extends AbstractSparkIntegrationTest {

    @BeforeAll
    static void startContainer() {
        spark = new GenericContainer<>(DockerImageName.parse("bitnami/spark:3.3.0"))
                .withCopyFileToContainer(MountableFile.forHostPath(Paths.get("target/test-classes/"), 0777), "/home")
                .withCopyFileToContainer(MountableFile.forHostPath(Paths.get("target/" + LIB_SPARK_CONNECTORS_AWS_DYNAMODB), 0445), "/home/" + LIB_SPARK_CONNECTORS_AWS_DYNAMODB)
                .withNetwork(network)
                .withEnv("AWS_ACCESS_KEY_ID", "test")
                .withEnv("AWS_SECRET_ACCESS_KEY", "test")
                .withEnv("SPARK_MODE", "master")
                .waitingFor(Wait.forLogMessage(".*Running Spark version.*", 1));
        spark.start();
    }

    @AfterAll
    static void stopContainer() {
        spark.stop();
    }

}
