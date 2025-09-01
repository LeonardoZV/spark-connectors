package com.leonardozv.spark.connectors.aws.sqs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.KeyUsageType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.*;

abstract class AbstractSparkIntegrationTest {

    protected static final String LIB_SPARK_CONNECTORS = "spark-connectors-aws-sqs-1.0.0.jar";

    protected static final List<String> dependencies = new ArrayList<>(Arrays.asList(
            "sqs-2.27.17.jar",
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
            "utils-2.27.17.jar",
            "amazon-sqs-java-extended-client-lib-2.1.1.jar",
            "payloadoffloading-common-2.2.0.jar",
            "s3-2.27.17.jar",
            "aws-json-protocol-2.27.17.jar",
            "aws-query-protocol-2.27.17.jar",
            "aws-xml-protocol-2.27.17.jar"
    ));

    protected static final Network network = Network.newNetwork();

    protected static GenericContainer<?> spark;

    @Container
    private final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withEnv("SQS_ENDPOINT_STRATEGY", "off")
            .withServices(SQS, S3, KMS);

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

    private SqsClient configureSqsClient() {

        return SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(SQS))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

    }

    private S3Client configureS3Client() {

        return S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

    }

    private KmsClient configureKmsClient() {

        return KmsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(KMS))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

    }

    private CreateQueueResponse configureQueue(SqsClient sqs, boolean isFIFO) {

        String queueName = "my-test";

        EnumMap<QueueAttributeName, String> queueAttributes = new EnumMap<>(QueueAttributeName.class);

        if(isFIFO) {
            queueName = queueName + ".fifo";
            queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
        }

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();

        return sqs.createQueue(createQueueRequest);

    }

    private CreateBucketResponse configureBucket(S3Client s3) {

        return s3.createBucket(CreateBucketRequest.builder().bucket("my-bucket").build());

    }

    private CreateKeyResponse configureKmsKey(KmsClient kms) {

        CreateKeyRequest createKeyRequest = CreateKeyRequest.builder()
                .description("Minha chave de criptografia KMS")
                .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
                .keySpec(KeySpec.SYMMETRIC_DEFAULT)
                .build();

        return kms.createKey(createKeyRequest);

    }

    private String getHostAccessibleQueueUrl(SqsClient sqs, String queueName) {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        return sqs.getQueueUrl(getQueueUrlRequest)
                .queueUrl()
                .replace("localstack", localstack.getHost())
                .replace("4566", localstack.getMappedPort(4566).toString());

    }

    private List<Message> getMessages(SqsClient sqs, boolean isFIFO) {

        String queueName = "my-test" + (isFIFO ? ".fifo": "");

        String queueUrl = getHostAccessibleQueueUrl(sqs, queueName);

        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .messageAttributeNames("All")
                .messageSystemAttributeNamesWithStrings("All")
                .build();

        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(request);

        return receiveMessageResponse.messages();

    }

    private ResponseInputStream<GetObjectResponse> getObject(S3Client s3, String key) {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("my-bucket").key(key).build();

        return s3.getObject(getObjectRequest);

    }

    private List<String> getLines(ResponseInputStream<GetObjectResponse> getObjectResponse) throws IOException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse));

        String line;

        List<String> lines = new java.util.ArrayList<>();

        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }

        return lines;

    }

    @Test
    void when_DataframeContainsValueColumn_should_PutAnSQSMessageUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        List<Message> messages = getMessages(sqs, false);

        assertThat(messages.get(0).body()).isEqualTo("my message body");

    }

    @Test
    void when_DataframeContainsValueColumnAndMultipleLines_should_PutAsManySQSMessagesInQueue() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/multiline_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        List<Message> messages = getMessages(sqs, false);

        assertThat(messages).hasSize(10);

    }

    @Test
    void when_DataframeContainsDataExceedsSQSSizeLimit_should_FailWholeBatch() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/large_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Batch requests cannot be longer than 1048576 bytes");

        List<Message> messages = getMessages(sqs, false);

        assertThat(messages).size().as("No messages should be written when the batch fails").isZero();

    }

    @Test
    void when_DataframeContainsLinesThatExceedsSQSMessageSizeLimit_should_ThrowAnException() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, false);
        EnumMap<QueueAttributeName, String> attributes = new EnumMap<>(QueueAttributeName.class);
        attributes.put(QueueAttributeName.MAXIMUM_MESSAGE_SIZE, Integer.toString(1024));
        SetQueueAttributesRequest setQueueAttributesRequest = SetQueueAttributesRequest.builder().queueUrl(getHostAccessibleQueueUrl(sqs, "my-test")).attributes(attributes).build();
        sqs.setQueueAttributes(setQueueAttributesRequest);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/multiline_large_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Some messages failed to be sent to the SQS queue");

        List<Message> messages = getMessages(sqs, false);

        assertThat(messages).as("Only messages up to 1024 should be written").hasSize(2);

    }

    @Test
    void when_DataframeContainsDelaySecondsColumn_should_PutAnSQSMessageWithDelaySecondsUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write_with_delay_seconds.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        List<Message> messages;

        messages = getMessages(sqs, false);
        assertThat(messages).isEmpty();

        TimeUnit.SECONDS.sleep(10);

        messages = getMessages(sqs, false);
        assertThat(messages).hasSize(4);

    }

    @Test
    void when_DataframeContainsMsgAttributesColumn_should_PutAnSQSMessageWithMessageAttributesUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write_with_message_attributes.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        List<Message> messages = getMessages(sqs, false);

        assertThat(messages.get(0).messageAttributes()).containsEntry("attribute", MessageAttributeValue.builder().dataType("String").stringValue("1000").build());
        assertThat(messages.get(0).attributes()).containsEntry(MessageSystemAttributeName.AWS_TRACE_HEADER, "test");

    }

    @Test
    void when_DataframeContainsMessageGroupIdAndDeduplicationIdColumn_should_PutAnSQSMessageWithMessageGroupIdAndDeduplicationIdUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, true);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write_with_message_group_id_and_deduplication_id.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        List<Message> messages = getMessages(sqs, true);

        assertThat(messages).hasSize(2);
        assertThat(messages.get(0).attributes()).containsEntry(MessageSystemAttributeName.MESSAGE_GROUP_ID, "id1");
        assertThat(messages.get(0).attributes()).containsEntry(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID, "id1");
        assertThat(messages.get(1).attributes()).containsEntry(MessageSystemAttributeName.MESSAGE_GROUP_ID, "id2");
        assertThat(messages.get(1).attributes()).containsEntry(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID, "id2");

    }

    @Test
    void when_WriterContainsUseSqsExtendedClientOption_should_PutAnSQSMessageAndS3ObjectWithSqsExtendedClientUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureSqsClient();
        configureQueue(sqs, false);
        S3Client s3 =  configureS3Client();
        configureBucket(s3);
        KmsClient kms = configureKmsClient();
        CreateKeyResponse createKeyResponse = configureKmsKey(kms);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write_with_sqs_extended_client.py", "http://localstack:4566", "http://localstack:4566", createKeyResponse.keyMetadata().arn());

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        Message message = getMessages(sqs, false).get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(message.body());
        JsonNode payloadNode = rootNode.get(1);
        String s3Key = payloadNode.get("s3Key").asText();
        assertThat(s3Key).startsWith("prefix/");

        ResponseInputStream<GetObjectResponse> stream = getObject(s3, s3Key);
        assertThat(stream.response().serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS);
        assertThat(stream.response().ssekmsKeyId()).isEqualTo(createKeyResponse.keyMetadata().arn());

        List<String> lines = getLines(stream);

        assertThat(lines.get(0)).isEqualTo("foo");

    }

}