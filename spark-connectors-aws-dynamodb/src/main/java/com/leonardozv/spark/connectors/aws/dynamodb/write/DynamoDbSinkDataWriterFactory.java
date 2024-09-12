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

        DynamoDbClient dynamodb = getAmazonDynamoDB();

        return new DynamoDbSinkDataWriter(partitionId, taskId, dynamodb, this.options);

    }

    private DynamoDbClient getAmazonDynamoDB() {

        DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();

        if (!this.options.endpoint().isEmpty())
            clientBuilder.region(Region.of(this.options.region())).endpointOverride(URI.create(this.options.endpoint()));
        else
            clientBuilder.region(Region.of(this.options.region()));

        return clientBuilder.build();

    }

}
