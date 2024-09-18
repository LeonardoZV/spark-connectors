package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

abstract class AbstractSparkIntegrationTest {

    protected static final String LIB_SPARK_CONNECTORS_AWS_DYNAMODB = "spark-connectors-aws-dynamodb-1.0.0.jar";

    protected static final Network network = Network.newNetwork();

    protected static GenericContainer<?> spark;

    @Container
    private final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withServices(DYNAMODB);

    private ExecResult executeSparkJob(String script, String... args) throws IOException, InterruptedException {

        String[] command = ArrayUtils.addAll(new String[] {"spark-submit", "--jars", "/home/" + LIB_SPARK_CONNECTORS_AWS_DYNAMODB, "--packages", "software.amazon.awssdk:dynamodb:2.27.17", "--master", "local", script}, args);

        ExecResult result = spark.execInContainer(command);

        System.out.println(result.getStdout());
        System.out.println(result.getStderr());

        return result;

    }

    private DynamoDbClient configureTable() {

        DynamoDbClient dynamodb = DynamoDbClient.builder()
                .endpointOverride(localstack.getEndpointOverride(DYNAMODB))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        String tableName = "my-table";

        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build())
                .build();

        dynamodb.createTable(createTableRequest);

        dynamodb.executeStatement(ExecuteStatementRequest.builder().statement("INSERT INTO \"my-table\" VALUE {'id': '123', 'name': 'John Doe', 'age': 30}").build());

        return dynamodb;

    }

    private GetItemResponse getItem(DynamoDbClient dynamodb, String key) {

        Map<String, AttributeValue> keyMap = new HashMap<>();

        keyMap.put("id", AttributeValue.builder().s(key).build());

        GetItemRequest request = GetItemRequest.builder().tableName("my-table").key(keyMap).build();

        return dynamodb.getItem(request);

    }

    @Test
    void when_DataframeContainsStatementColumn_should_ExecuteStatementUsingSpark() throws IOException, InterruptedException {

        // arrange
        DynamoDbClient dynamodb = configureTable();

        // act
        ExecResult result = executeSparkJob("/home/scripts/dynamodb_write.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        GetItemResponse response = getItem(dynamodb, "123");

        assertThat(response.hasItem()).isTrue();
        assertThat(response.item().get("id").s()).isEqualTo("123");
        assertThat(response.item().get("name").s()).isEqualTo("John Doe");
        assertThat(response.item().get("age").n()).isEqualTo("31");

    }

    @Test
    void when_DataframeContainsStatementColumnAndErrorsToIgnoreOption_should_ExecuteStatementUsingSpark() throws IOException, InterruptedException {

        // arrange
        configureTable();

        // act
        ExecResult result = executeSparkJob("/home/scripts/dynamodb_write_with_errors_to_ignore.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

    }

}