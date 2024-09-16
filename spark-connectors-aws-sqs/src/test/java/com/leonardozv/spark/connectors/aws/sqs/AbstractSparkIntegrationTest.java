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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

abstract class AbstractSparkIntegrationTest {

    protected static final String LIB_SPARK_CONNECTORS_AWS_SQS = "spark-connectors-aws-sqs-1.0.0.jar";

    protected static final Network network = Network.newNetwork();

    protected static GenericContainer<?> spark;

    @Container
    private final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withEnv("SQS_ENDPOINT_STRATEGY", "off")
            .withServices(SQS, S3);

    public ExecResult executeSparkSubmit(String script, String... args) throws IOException, InterruptedException {

        String[] command = ArrayUtils.addAll(new String[] {"spark-submit", "--jars", "/home/" + LIB_SPARK_CONNECTORS_AWS_SQS, "--packages", "software.amazon.awssdk:sqs:2.27.17,software.amazon.awssdk:s3:2.27.17,com.amazonaws:amazon-sqs-java-extended-client-lib:2.1.1", "--master", "local", script}, args);

        ExecResult result = spark.execInContainer(command);

        System.out.println(result.getStdout());
        System.out.println(result.getStderr());

        return result;

    }

    private SqsClient configureQueue(boolean isFIFO) {

        SqsClient sqs = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(SQS))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        String queueName = "my-test";

        Map<QueueAttributeName, String> queueAttributes = new HashMap<>();

        if(isFIFO) {
            queueName = queueName + ".fifo";
            queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
            queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true");
        }

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();

        sqs.createQueue(createQueueRequest);

        return sqs;

    }

    private S3Client configureBucket() {

        S3Client s3 = S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();

        s3.createBucket(CreateBucketRequest.builder().bucket("my-bucket").build());

        return s3;

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

    private List<String> getLines(S3Client s3, String key) throws IOException {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("my-bucket").key(key).build();

        ResponseInputStream<?> getObjectResponse = s3.getObject(getObjectRequest);

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
        SqsClient sqs = configureQueue(false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessages(sqs, false).get(0);
        assertThat(message.body()).isEqualTo("my message body");

    }

    @Test
    void when_DataframeContainsValueColumnAndMultipleLines_should_PutAsManySQSMessagesInQueue() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue(false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/multiline_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        List<Message> messages = getMessages(sqs, false);
        assertThat(messages).size().isEqualTo(10);

    }

    @Test
    void when_DataframeContainsDataExceedsSQSSizeLimit_should_FailWholeBatch() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue(false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/large_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Batch requests cannot be longer than 262144 bytes");
        List<Message> messages = getMessages(sqs, false);
        assertThat(messages).size().as("No messages should be written when the batch fails").isZero();

    }

    @Test
    void when_DataframeContainsLinesThatExceedsSQSMessageSizeLimit_should_ThrowAnException() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue(false);
        HashMap<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.MAXIMUM_MESSAGE_SIZE, Integer.toString(1024));
        SetQueueAttributesRequest setQueueAttributesRequest = SetQueueAttributesRequest.builder().queueUrl(getHostAccessibleQueueUrl(sqs, "my-test")).attributes(attributes).build();
        sqs.setQueueAttributes(setQueueAttributesRequest);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write.py", "/home/data/multiline_large_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Some messages failed to be sent to the SQS queue");
        List<Message> messages = getMessages(sqs, false);
        assertThat(messages).size().as("Only messages up to 1024 should be written").isEqualTo(2);

    }

    @Test
    void when_DataframeContainsGroupIdColumn_should_PutAnSQSMessageWithMessageGroupIdUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue(true);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write_with_group_id.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessages(sqs, true).get(0);
        assertThat(message.attributes()).containsKey(MessageSystemAttributeName.MESSAGE_GROUP_ID).containsValue("id1");

    }

    @Test
    void when_DataframeContainsMsgAttributesColumn_should_PutAnSQSMessageWithMessageAttributesUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue(false);

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write_with_msg_attributes.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessages(sqs, false).get(0);
        assertThat(message.messageAttributes().get("attribute-a").stringValue()).isEqualTo("1000");
        assertThat(message.messageAttributes().get("attribute-b").stringValue()).isEqualTo("2000");

    }

    @Test
    void when_WriterContainsUseSqsExtendedClientOption_should_PutAnSQSMessageAndS3ObjectWithSqsExtendedClientUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue(false);
        S3Client s3 = configureBucket();

        // act
        ExecResult result = executeSparkSubmit("/home/scripts/sqs_write_with_sqs_extended_client.py", "http://localstack:4566", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        Message message = getMessages(sqs, false).get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(message.body());
        JsonNode payloadNode = rootNode.get(1);
        String s3key = payloadNode.get("s3Key").asText();
        String line = getLines(s3, s3key).get(0);
        assertThat(line).isEqualTo("foo");

    }

}