package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SQSSinkDataWriterFactoryUnitTest {

    @Test
    void testCreateWriterWithSQSEndpoint() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region("us-east-1")
                .sqsEndpoint("http://localhost:4566")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
            SqsClient mockClient = mock(SqsClient.class);
            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);

            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);
            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");

            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);

            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            assertNotNull(writer);
            assertInstanceOf(SqsSinkDataWriter.class, writer);
        }

    }

    @Test
    void testCreateWriterWithoutSQSEndpoint() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region("us-east-1")
                .sqsEndpoint("")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
            SqsClient mockClient = mock(SqsClient.class);
            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);

            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);
            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");

            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);

            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            assertNotNull(writer);
            assertInstanceOf(SqsSinkDataWriter.class, writer);
        }

    }

    @Test
    void testCreateWriterWithQueueOwnerAWSAccountId() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region("us-east-1")
                .sqsEndpoint("http://localhost:4566")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
            SqsClient mockClient = mock(SqsClient.class);
            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);

            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);
            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");

            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);

            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            assertNotNull(writer);
            assertInstanceOf(SqsSinkDataWriter.class, writer);
        }

    }

    @Test
    void testCreateWriterWithExtendedClient() throws Exception {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region("us-east-1")
                .sqsEndpoint("http://localhost:4566")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .useSqsExtendedClient(true)
                .s3Endpoint("")
                .bucketName("")
                .payloadSizeThreshold(-1)
                .build();

        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
            SqsClient mockClient = mock(SqsClient.class);
            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);

            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);
            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");

            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);

            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            assertNotNull(writer);
            assertInstanceOf(SqsSinkDataWriter.class, writer);
        }

    }

    @Test
    void testCreateWriterWithS3Endpoint() throws Exception {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region("us-east-1")
                .sqsEndpoint("http://localhost:4566")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .useSqsExtendedClient(true)
                .s3Endpoint("http://localhost:4566")
                .bucketName("test-bucket")
                .build();

        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
            SqsClient mockClient = mock(SqsClient.class);
            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);

            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);
            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");

            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);

            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            assertNotNull(writer);
            assertInstanceOf(SqsSinkDataWriter.class, writer);
        }

    }

    @Test
    void testCreateWriterWithBucketName() throws Exception {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region("us-east-1")
                .sqsEndpoint("http://localhost:4566")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .useSqsExtendedClient(true)
                .s3Endpoint("")
                .bucketName("test-bucket")
                .build();

        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
            SqsClient mockClient = mock(SqsClient.class);
            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);

            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);
            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");

            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);

            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            assertNotNull(writer);
            assertInstanceOf(SqsSinkDataWriter.class, writer);
        }

    }

    @Test
    void testCreateWriterWithPayloadSizeThreshold() throws Exception {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region("us-east-1")
                .sqsEndpoint("http://localhost:4566")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .useSqsExtendedClient(true)
                .s3Endpoint("")
                .bucketName("test-bucket")
                .payloadSizeThreshold(256)
                .build();

        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
            SqsClient mockClient = mock(SqsClient.class);
            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);

            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockClient);
            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");

            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);

            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            assertNotNull(writer);
            assertInstanceOf(SqsSinkDataWriter.class, writer);
        }

    }

}