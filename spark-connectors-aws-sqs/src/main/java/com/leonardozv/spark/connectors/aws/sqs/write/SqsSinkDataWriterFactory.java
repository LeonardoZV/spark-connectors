package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import software.amazon.awssdk.auth.credentials.*;
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

        SqsClient sqsClient;

        if (this.options.useSqsExtendedClient()) {

            try {

                Class<?> extendedClientConfigClass = Class.forName("com.amazon.sqs.javamessaging.ExtendedClientConfiguration");
                Object extendedClientConfig = getExtendedClientConfiguration();

                Class<?> amazonSQSExtendedClientClass = Class.forName("com.amazon.sqs.javamessaging.AmazonSQSExtendedClient");
                sqsClient = (SqsClient) amazonSQSExtendedClientClass.getConstructor(SqsClient.class, extendedClientConfigClass).newInstance(getSqsClient(), extendedClientConfig);

            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new RuntimeException("AmazonSQSExtendedClient class not found or could not be instantiated", e);
            }

        } else {
            sqsClient = getSqsClient();
        }

        GetQueueUrlRequest queueUrlRequest = getGetQueueUrlRequest();

        String queueUrl = sqsClient.getQueueUrl(queueUrlRequest).queueUrl();

        return new SqsSinkDataWriter(partitionId, taskId, sqsClient, queueUrl, this.options);

    }

    private AwsCredentialsProvider identityCredentialsProvider(String credentialsProvider, String profile, String accessKeyId, String secretAccessKey, String sessionToken) {

        switch (credentialsProvider) {

            case "SystemPropertyCredentialsProvider":
                return SystemPropertyCredentialsProvider.create();

            case "EnvironmentVariableCredentialsProvider":
                return EnvironmentVariableCredentialsProvider.create();

            case "WebIdentityTokenFileCredentialsProvider":
                return WebIdentityTokenFileCredentialsProvider.create();

            case "ProfileCredentialsProvider":
                if (profile.isEmpty())
                    return ProfileCredentialsProvider.create();
                else
                    return ProfileCredentialsProvider.create(profile);

            case "ContainerCredentialsProvider":
                return ContainerCredentialsProvider.create();

            case "InstanceProfileCredentialsProvider":
                return InstanceProfileCredentialsProvider.create();

            case "StaticCredentialsProvider":
                AwsCredentials credentials;
                if (sessionToken.isEmpty())
                    credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
                else
                    credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
                return StaticCredentialsProvider.create(credentials);

            case "AnonymousCredentialsProvider":
                return AnonymousCredentialsProvider.create();

            default:
                return DefaultCredentialsProvider.create();

        }

    }

    private SqsClient getSqsClient() {

        SqsClientBuilder clientBuilder = SqsClient.builder();

        clientBuilder.credentialsProvider(identityCredentialsProvider(this.options.credentialsProvider(), this.options.profile(), this.options.accessKeyId(), this.options.secretAccessKey(), this.options.sessionToken()));

        clientBuilder.region(Region.of(this.options.region()));

        if (!this.options.endpoint().isEmpty())
            clientBuilder.endpointOverride(URI.create(this.options.endpoint()));

        return clientBuilder.build();

    }

    private GetQueueUrlRequest getGetQueueUrlRequest() {

        GetQueueUrlRequest.Builder getQueueUrlRequestBuilder = GetQueueUrlRequest.builder().queueName(this.options.queueName());

        if (!this.options.queueOwnerAWSAccountId().isEmpty())
            getQueueUrlRequestBuilder.queueOwnerAWSAccountId(this.options.queueOwnerAWSAccountId());

        return getQueueUrlRequestBuilder.build();

    }

    private Object getS3Client() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Class<?> s3ClientClass = Class.forName("software.amazon.awssdk.services.s3.S3Client");
        Class<?> s3ClientBuilderClass = Class.forName("software.amazon.awssdk.services.s3.S3ClientBuilder");
        Object clientBuilder = s3ClientClass.getMethod("builder").invoke(null);

        s3ClientBuilderClass.getMethod("credentialsProvider", AwsCredentialsProvider.class).invoke(clientBuilder, identityCredentialsProvider(this.options.s3CredentialsProvider(), this.options.s3Profile(), this.options.s3AccessKeyId(), this.options.s3SecretAccessKey(), this.options.s3SessionToken()));

        s3ClientBuilderClass.getMethod("region", Region.class).invoke(clientBuilder, Region.of(this.options.s3Region()));

        if (!this.options.s3Endpoint().isEmpty())
            s3ClientBuilderClass.getMethod("endpointOverride", URI.class).invoke(clientBuilder, URI.create(this.options.s3Endpoint()));

        s3ClientBuilderClass.getMethod("forcePathStyle", Boolean.class).invoke(clientBuilder, this.options.forcePathStyle());

        return s3ClientBuilderClass.getMethod("build").invoke(clientBuilder);

    }

    private Object getExtendedClientConfiguration() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Class<?> s3ClientClass = Class.forName("software.amazon.awssdk.services.s3.S3Client");
        Class<?> extendedClientConfigClass = Class.forName("com.amazon.sqs.javamessaging.ExtendedClientConfiguration");
        Object extendedClientConfig = extendedClientConfigClass.getConstructor().newInstance();

        if (!this.options.bucketName().isEmpty())
            extendedClientConfigClass.getMethod("setPayloadSupportEnabled", s3ClientClass, String.class).invoke(extendedClientConfig, getS3Client(), this.options.bucketName());

        if (this.options.payloadSizeThreshold() >= 0)
            extendedClientConfigClass.getMethod("setPayloadSizeThreshold", int.class).invoke(extendedClientConfig, this.options.payloadSizeThreshold());

        if (!this.options.s3KeyPrefix().isEmpty())
            extendedClientConfigClass.getMethod("setS3KeyPrefix", String.class).invoke(extendedClientConfig, this.options.s3KeyPrefix());

        return extendedClientConfig;

    }

}
