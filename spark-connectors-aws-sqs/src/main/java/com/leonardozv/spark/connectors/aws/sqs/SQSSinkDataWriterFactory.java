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

        SqsClient sqs;

        if(this.options.useSqsExtendedClient()) {

            ExtendedClientConfiguration extendedClientConfig = new ExtendedClientConfiguration();

            if(!this.options.bucketName().isEmpty()){
                S3Client s3 = getAmazonS3();
                extendedClientConfig.setPayloadSupportEnabled(s3, this.options.bucketName());
            }

            if (this.options.payloadSizeThreshold() > 0) {
                extendedClientConfig.setPayloadSizeThreshold(this.options.payloadSizeThreshold());
            }

            sqs = new AmazonSQSExtendedClient(getAmazonSQS(), extendedClientConfig);

        } else {

            sqs = getAmazonSQS();

        }

        GetQueueUrlRequest queueUrlRequest = getGetQueueUrlRequest();

        String queueUrl = sqs.getQueueUrl(queueUrlRequest).queueUrl();

        return new SQSSinkDataWriter(partitionId,
                taskId,
                sqs,
                options.batchSize(),
                queueUrl,
                options.valueColumnIndex(),
                options.msgAttributesColumnIndex(),
                options.groupIdColumnIndex());
    }

    private S3Client getAmazonS3() {
        S3ClientBuilder clientBuilder = S3Client.builder();
        if (!options.endpoint().isEmpty())
            clientBuilder.region(Region.of(options.region())).endpointOverride(URI.create(options.endpoint()));
        else
            clientBuilder.region(Region.of(options.region()));
        return clientBuilder.build();
    }

    private SqsClient getAmazonSQS() {
        SqsClientBuilder clientBuilder = SqsClient.builder();
        if (!options.endpoint().isEmpty())
            clientBuilder.region(Region.of(options.region())).endpointOverride(URI.create(options.endpoint()));
        else
            clientBuilder.region(Region.of(options.region()));
        return clientBuilder.build();
    }

    private GetQueueUrlRequest getGetQueueUrlRequest() {
        GetQueueUrlRequest.Builder getQueueUrlRequestBuilder = GetQueueUrlRequest.builder().queueName(options.queueName());
        if (!options.queueOwnerAWSAccountId().isEmpty()) {
            getQueueUrlRequestBuilder.queueOwnerAWSAccountId(options.queueOwnerAWSAccountId());
        }
        return getQueueUrlRequestBuilder.build();
    }

}
