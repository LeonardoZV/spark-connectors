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
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

abstract class AbstractSparkIntegrationTest {

    protected static final String LIB_SPARK_CONNECTORS = "spark-connectors-aws-dynamodb-1.0.0.jar";

    protected static final List<String> dependencies = new ArrayList<>(Arrays.asList(
            "dynamodb-2.27.17.jar",
            "annotations-2.27.17.jar",
            "apache-client-2.27.17.jar",
            "auth-2.27.17.jar",
            "aws-core-2.27.17.jar",
            "aws-json-protocol-2.27.17.jar",
            "checksums-2.27.17.jar",
            "checksums-spi-2.27.17.jar",
            "endpoints-spi-2.27.17.jar",
            "eventstream-1.0.1.jar",
            "http-auth-2.27.17.jar",
            "http-auth-aws-2.27.17.jar",
            "http-auth-aws-eventstream-2.27.17.jar",
            "http-auth-spi-2.27.17.jar",
            "http-client-spi-2.27.17.jar",
            "identity-spi-2.27.17.jar",
            "json-utils-2.27.17.jar",
            "metrics-spi-2.27.17.jar",
            "profiles-2.27.17.jar",
            "protocol-core-2.27.17.jar",
            "reactive-streams-1.0.4.jar",
            "regions-2.27.17.jar",
            "retries-2.27.17.jar",
            "retries-spi-2.27.17.jar",
            "sdk-core-2.27.17.jar",
            "slf4j-api-1.7.36.jar",
            "third-party-jackson-core-2.27.17.jar",
            "utils-2.27.17.jar"
    ));

    protected static final Network network = Network.newNetwork();

    protected static GenericContainer<?> spark;

    @Container
    private final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withServices(DYNAMODB);

    public ExecResult executeSparkSubmit(String script, String... args) throws IOException, InterruptedException {

        String[] command = ArrayUtils.addAll(new String[] {"spark-submit", "--jars", "/home/libs/" + LIB_SPARK_CONNECTORS, "--packages", "software.amazon.awssdk:sqs:2.27.17,com.amazonaws:amazon-sqs-java-extended-client-lib:2.1.1,software.amazon.awssdk:s3:2.27.17", "--master", "local", script}, args);

        ExecResult result = spark.execInContainer(command);

        System.out.println(result.getStdout());
        System.out.println(result.getStderr());

        return result;

    }

    public ExecResult executeSparkSubmitJars(String script, String... args) throws IOException, InterruptedException {

        String dependenciesContainerPath =  "/home/libs/" + LIB_SPARK_CONNECTORS + "," + dependencies.stream().map(element -> "/home/libs/" + element).collect(Collectors.joining(","));

        String[] command = ArrayUtils.addAll(new String[] {"spark-submit", "--jars", dependenciesContainerPath, "--master", "local", script}, args);

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
        ExecResult result = executeSparkSubmit("/home/scripts/dynamodb_write.py", "http://localstack:4566");

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
        ExecResult result = executeSparkSubmit("/home/scripts/dynamodb_write_with_errors_to_ignore.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

    }

}