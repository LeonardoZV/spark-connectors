package com.leonardozv.spark.connectors.aws.sqs;

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

import java.net.URI;

public class SQSSinkDataWriterFactory implements DataWriterFactory {

    private final SQSSinkOptions options;

    public SQSSinkDataWriterFactory(SQSSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        final S3Client s3 = getAmazonS3();
        final ExtendedClientConfiguration extendedClientConfig = new ExtendedClientConfiguration().withPayloadSupportEnabled(s3, options.getBucketName());
        final SqsClient sqs = new AmazonSQSExtendedClient(getAmazonSQS(), extendedClientConfig);
        final GetQueueUrlRequest queueUrlRequest = getGetQueueUrlRequest();
        final String queueUrl = sqs.getQueueUrl(queueUrlRequest).queueUrl();
        return new SQSSinkDataWriter(partitionId,
                taskId,
                sqs,
                options.getBatchSize(),
                queueUrl,
                options.getValueColumnIndex(),
                options.getMsgAttributesColumnIndex(),
                options.getGroupIdColumnIndex());
    }

    private S3Client getAmazonS3() {
        S3ClientBuilder clientBuilder = S3Client.builder();
        if (!options.getEndpoint().isEmpty())
            clientBuilder.region(Region.of(options.getRegion())).endpointOverride(URI.create(options.getEndpoint()));
        else
            clientBuilder.region(Region.of(options.getRegion()));
        return clientBuilder.build();
    }

    private SqsClient getAmazonSQS() {
        SqsClientBuilder clientBuilder = SqsClient.builder();
        if (!options.getEndpoint().isEmpty())
            clientBuilder.region(Region.of(options.getRegion())).endpointOverride(URI.create(options.getEndpoint()));
        else
            clientBuilder.region(Region.of(options.getRegion()));
        return clientBuilder.build();
    }

    private GetQueueUrlRequest getGetQueueUrlRequest() {
        GetQueueUrlRequest.Builder getQueueUrlRequestBuilder = GetQueueUrlRequest.builder().queueName(options.getQueueName());
        if (!options.getQueueOwnerAWSAccountId().isEmpty()) {
            getQueueUrlRequestBuilder.queueOwnerAWSAccountId(options.getQueueOwnerAWSAccountId());
        }
        return getQueueUrlRequestBuilder.build();
    }

}
