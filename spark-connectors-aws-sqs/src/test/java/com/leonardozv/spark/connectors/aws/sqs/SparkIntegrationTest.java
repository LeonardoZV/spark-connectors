package com.leonardozv.spark.connectors.aws.sqs;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

public abstract class SparkIntegrationTest {

    private static final Network network = Network.newNetwork();
    private static final String LIB_SPARK_CONNECTORS_AWS_SQS = "spark-connectors-aws-sqs-1.0.0.jar";
    private static final String LIB_AWS_SQS = "sqs-2.27.17.jar";
    private static final String LIB_AWS_S3 = "s3-2.27.17.jar";
    private static final String LIB_AWS_SQS_EXTENDED_CLIENT = "amazon-sqs-java-extended-client-lib-2.1.1.jar";

    @Container
    private final GenericContainer spark;

    @Container
    private final LocalStackContainer localstack;

    public SparkIntegrationTest(String sparkImage) {
        spark = new GenericContainer(DockerImageName.parse(sparkImage))
                .withCopyFileToContainer(MountableFile.forHostPath("build/resources/test/.", 0777), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/" + LIB_SPARK_CONNECTORS_AWS_SQS, 0445), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/deps/" + LIB_AWS_SQS, 0445), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/deps/" + LIB_AWS_S3, 0445), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/deps/" + LIB_AWS_SQS_EXTENDED_CLIENT, 0445), "/home/")
                .withNetwork(network)
                .withEnv("AWS_ACCESS_KEY_ID", "test")
                .withEnv("AWS_SECRET_KEY", "test")
                .withEnv("SPARK_MODE", "master");
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                .withNetwork(network)
                .withNetworkAliases("localstack")
                .withEnv("SQS_ENDPOINT_STRATEGY", "off")
                .withServices(SQS);
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

    private SqsClient configureQueue() {
        return configureQueue(false);
    }

    private ExecResult execSparkJob(String script, String... args) throws IOException, InterruptedException {
        String[] command = ArrayUtils.addAll(new String[] {"spark-submit", "--jars", "/home/" + LIB_SPARK_CONNECTORS_AWS_SQS + ",/home/" + LIB_AWS_SQS + ",/home/" + LIB_AWS_S3 + ",/home/" + LIB_AWS_SQS_EXTENDED_CLIENT, "--master", "local", script}, args);
        ExecResult result = spark.execInContainer(command);
        System.out.println(result.getStdout());
        System.out.println(result.getStderr());
        return result;
    }

    private String getHostAccessibleQueueUrl(SqsClient sqs, String queueName) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
        return sqs.getQueueUrl(getQueueUrlRequest).queueUrl().replace("localstack", localstack.getHost()).replace("4566", localstack.getMappedPort(4566).toString());
    }

    private List<Message> getMessagesPut(SqsClient sqs, boolean isFIFO) {
        String queueName = "my-test" + (isFIFO ? ".fifo": "");
        String queueUrl = getHostAccessibleQueueUrl(sqs, queueName);
        ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).messageAttributeNames("All").build();
        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(request);
        return receiveMessageResponse.messages();
    }

    private List<Message> getMessagesPut(SqsClient sqs){
        return getMessagesPut(sqs, false);
    }

    @Test
    void when_DataframeContainsValueColumn_should_PutAnSQSMessageUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue();

        // act
        ExecResult result = execSparkJob("/home/sqs_write.py","/home/sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        Message message = getMessagesPut(sqs).get(0);

        assertThat(message.body()).isEqualTo("my message body");

    }

    @Test
    void when_DataframeContainsValueColumnAndMultipleLines_should_PutAsManySQSMessagesInQueue() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue();

        // act
        ExecResult result = execSparkJob("/home/sqs_write.py", "/home/multiline_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();

        List<Message> messages = getMessagesPut(sqs);

        assertThat(messages).size().isEqualTo(10);

    }

    @Test
    void when_DataframeContainsDataExceedsSQSSizeLimit_should_FailWholeBatch() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue();

        // act
        ExecResult result = execSparkJob("/home/sqs_write.py", "/home/large_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Batch requests cannot be longer than 262144 bytes");

        List<Message> messages = getMessagesPut(sqs);

        assertThat(messages).size().as("No messages should be written when the batch fails").isZero();

    }

    @Test
    void when_DataframeContainsLinesThatExceedsSQSMessageSizeLimit_should_ThrowAnException() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue();
        HashMap<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.MAXIMUM_MESSAGE_SIZE, Integer.toString(1024));
        SetQueueAttributesRequest setQueueAttributesRequest = SetQueueAttributesRequest.builder().queueUrl(getHostAccessibleQueueUrl(sqs, "my-test")).attributes(attributes).build();
        sqs.setQueueAttributes(setQueueAttributesRequest);

        // act
        ExecResult result = execSparkJob("/home/sqs_write.py", "/home/multiline_large_sample.txt", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Some messages failed to be sent to the SQS queue");
        List<Message> messages = getMessagesPut(sqs);
        assertThat(messages).size().as("Only messages up to 1024 should be written").isEqualTo(2);

    }

    @Test
    void when_DataframeContainsGroupIdColumn_should_PutAnSQSMessageWithMessageGroupIdUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue(true);

        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_groupid.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs, true).get(0);
        assertThat(message.attributes()).containsKey(MessageSystemAttributeName.MESSAGE_GROUP_ID).containsValue("id1");

    }

    @Test
    void when_DataframeContainsMsgAttributesColumn_should_PutAnSQSMessageWithMessageAttributesUsingSpark() throws IOException, InterruptedException {

        // arrange
        SqsClient sqs = configureQueue();

        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_msgattribs.py", "http://localstack:4566");

        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs).get(0);
        assertThat(message.messageAttributes().get("attribute-a").stringValue()).isEqualTo("1000");
        assertThat(message.messageAttributes().get("attribute-b").stringValue()).isEqualTo("2000");

    }

}