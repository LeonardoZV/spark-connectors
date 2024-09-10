package com.leonardozv.spark.connectors.aws.dynamodb.write;

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
                options.batchSize(),
                options.errorsToIgnore(),
                options.statementColumnIndex());
    }

    private DynamoDbClient getAmazonDynamoDB() {
        DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();
        if (!options.endpoint().isEmpty())
            clientBuilder.region(Region.of(options.region())).endpointOverride(URI.create(options.endpoint()));
        else
            clientBuilder.region(Region.of(options.region()));
        return clientBuilder.build();
    }

}
