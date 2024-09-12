package com.leonardozv.spark.connectors.aws.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class Spark3_3_0IntegrationTest extends SparkIntegrationTest {

    public Spark3_3_0IntegrationTest() {
        super("bitnami/spark:3.3.0");
    }

}