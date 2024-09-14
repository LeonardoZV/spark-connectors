package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoDbSinkDataWriterFactoryUnitTest {

    @Test
    void testCreateWriter() {

        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        // Create DynamoDbSinkDataWriterFactory
        DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

        // Mock DynamoDbClient and DynamoDbClientBuilder
        DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
        DynamoDbClientBuilder clientBuilder = mock(DynamoDbClientBuilder.class);
        when(clientBuilder.region(any(Region.class))).thenReturn(clientBuilder);
        when(clientBuilder.endpointOverride(any(URI.class))).thenReturn(clientBuilder);
        when(clientBuilder.build()).thenReturn(dynamoDbClient);

        // Mock static method DynamoDbClient.builder()
        try (MockedStatic<DynamoDbClient> mockedDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {
            mockedDynamoDbClient.when(DynamoDbClient::builder).thenReturn(clientBuilder);

            // Create DataWriter
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Verify the writer is created
            assertNotNull(writer, "DataWriter should not be null");

            // Verify DynamoDbClient configuration
            verify(clientBuilder).region(Region.of("us-west-2"));
            verify(clientBuilder).endpointOverride(URI.create("http://localhost:8000"));
        }

    }

    @Test
    void testCreateWithoutEndpoint() {

        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .endpoint("")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        // Create DynamoDbSinkDataWriterFactory
        DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

        // Mock DynamoDbClient and DynamoDbClientBuilder
        DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
        DynamoDbClientBuilder clientBuilder = mock(DynamoDbClientBuilder.class);
        when(clientBuilder.region(any(Region.class))).thenReturn(clientBuilder);
        when(clientBuilder.endpointOverride(any(URI.class))).thenReturn(clientBuilder);
        when(clientBuilder.build()).thenReturn(dynamoDbClient);

        // Mock static method DynamoDbClient.builder()
        try (MockedStatic<DynamoDbClient> mockedDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {
            mockedDynamoDbClient.when(DynamoDbClient::builder).thenReturn(clientBuilder);

            // Create DataWriter
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Verify the writer is created
            assertNotNull(writer, "DataWriter should not be null");

            // Verify DynamoDbClient configuration
            verify(clientBuilder).region(Region.of("us-west-2"));
        }

    }

}
