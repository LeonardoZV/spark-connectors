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
class Spark3_2_1IntegrationTest extends AbstractSparkIntegrationTest {

    @BeforeAll
    static void startContainer() {
        spark = new GenericContainer<>(DockerImageName.parse("bitnami/spark:3.2.1"))
                .withCopyFileToContainer(MountableFile.forHostPath(Paths.get("target/test-classes/"), 0777), "/home")
                .withCopyFileToContainer(MountableFile.forHostPath(Paths.get("target/" + LIB_SPARK_CONNECTORS), 0445), "/home/libs/" + LIB_SPARK_CONNECTORS)
                .withCopyFileToContainer(MountableFile.forHostPath(Paths.get("target/libs/"), 0777), "/home/libs")
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
