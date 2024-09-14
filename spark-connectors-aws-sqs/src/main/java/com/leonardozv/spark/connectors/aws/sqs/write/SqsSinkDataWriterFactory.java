package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;

public class SqsSinkDataWriterFactory implements DataWriterFactory {

    private final SqsSinkOptions options;

    public SqsSinkDataWriterFactory(SqsSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        SqsClient sqs;

        if (this.options.useSqsExtendedClient()) {

            try {

                Class<?> s3ClientClass = Class.forName("software.amazon.awssdk.services.s3.S3Client");
                Class<?> extendedClientConfigClass = Class.forName("com.amazon.sqs.javamessaging.ExtendedClientConfiguration");
                Object extendedClientConfig = extendedClientConfigClass.getConstructor().newInstance();

                if (!this.options.bucketName().isEmpty()) {
                    Object s3 = getAmazonS3();
                    extendedClientConfigClass.getMethod("setPayloadSupportEnabled", s3ClientClass, String.class).invoke(extendedClientConfig, s3, this.options.bucketName());
                }

                if (this.options.payloadSizeThreshold() >= 0) {
                    extendedClientConfigClass.getMethod("setPayloadSizeThreshold", int.class).invoke(extendedClientConfig, this.options.payloadSizeThreshold());
                }

                Class<?> amazonSQSExtendedClientClass = Class.forName("com.amazon.sqs.javamessaging.AmazonSQSExtendedClient");
                sqs = (SqsClient) amazonSQSExtendedClientClass.getConstructor(SqsClient.class, extendedClientConfigClass).newInstance(getAmazonSQS(), extendedClientConfig);

            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new RuntimeException("AmazonSQSExtendedClient class not found or could not be instantiated", e);
            }

        } else {
            sqs = getAmazonSQS();
        }

        GetQueueUrlRequest queueUrlRequest = getGetQueueUrlRequest();

        String queueUrl = sqs.getQueueUrl(queueUrlRequest).queueUrl();

        return new SqsSinkDataWriter(partitionId, taskId, sqs, queueUrl, this.options);

    }

    private Object getAmazonS3() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Class<?> s3ClientClass = Class.forName("software.amazon.awssdk.services.s3.S3Client");
        Class<?> s3ClientBuilderClass = Class.forName("software.amazon.awssdk.services.s3.S3ClientBuilder");
        Object clientBuilder = s3ClientClass.getMethod("builder").invoke(null);

        if (!this.options.s3Endpoint().isEmpty()) {
            s3ClientBuilderClass.getMethod("region", Region.class).invoke(clientBuilder, Region.of(this.options.region()));
            s3ClientBuilderClass.getMethod("endpointOverride", URI.class).invoke(clientBuilder, URI.create(this.options.s3Endpoint()));
        } else {
            s3ClientBuilderClass.getMethod("region", Region.class).invoke(clientBuilder, Region.of(this.options.region()));
        }

        s3ClientBuilderClass.getMethod("forcePathStyle", Boolean.class).invoke(clientBuilder, this.options.forcePathStyle());

        return s3ClientBuilderClass.getMethod("build").invoke(clientBuilder);

    }

    private SqsClient getAmazonSQS() {

        SqsClientBuilder clientBuilder = SqsClient.builder();

        clientBuilder.region(Region.of(this.options.region()));

        if (!this.options.sqsEndpoint().isEmpty())
            clientBuilder.endpointOverride(URI.create(this.options.sqsEndpoint()));

        return clientBuilder.build();

    }

    private GetQueueUrlRequest getGetQueueUrlRequest() {

        GetQueueUrlRequest.Builder getQueueUrlRequestBuilder = GetQueueUrlRequest.builder().queueName(this.options.queueName());

        if (!this.options.queueOwnerAWSAccountId().isEmpty()) {
            getQueueUrlRequestBuilder.queueOwnerAWSAccountId(this.options.queueOwnerAWSAccountId());
        }

        return getQueueUrlRequestBuilder.build();

    }

}
