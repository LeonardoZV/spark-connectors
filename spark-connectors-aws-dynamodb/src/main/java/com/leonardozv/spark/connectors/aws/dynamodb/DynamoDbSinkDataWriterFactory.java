package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;

public class DynamoDbSinkDataWriterFactory implements DataWriterFactory {

    private final DynamoDbSinkOptions options;

    public DynamoDbSinkDataWriterFactory(DynamoDbSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        final DynamoDbClient dynamodb = getAmazonDynamoDB();
        return new DynamoDbSinkDataWriter(partitionId,
                taskId,
                dynamodb,
                options.getBatchSize(),
                options.getTreatConditionalCheckFailedAsError(),
                options.getStatementColumnIndex());
    }

    private DynamoDbClient getAmazonDynamoDB() {
        DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();
        if (!options.getEndpoint().isEmpty())
            clientBuilder.region(Region.of(options.getRegion())).endpointOverride(URI.create(options.getEndpoint()));
        else
            clientBuilder.region(Region.of(options.getRegion()));
        return clientBuilder.build();
    }

}
