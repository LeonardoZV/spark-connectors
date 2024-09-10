package com.leonardozv.spark.connectors.aws.sqs;

//import org.apache.spark.sql.catalyst.InternalRow;
//import org.apache.spark.sql.connector.write.DataWriter;
//import org.junit.jupiter.api.Test;
//import org.mockito.MockedConstruction;
//import org.mockito.MockedStatic;
//import org.mockito.Mockito;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.sqs.SqsClient;
//import software.amazon.awssdk.services.sqs.SqsClientBuilder;
//import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
//import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
//
//import java.net.URI;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.mockito.Mockito.*;
//
//class SQSSinkDataWriterFactoryTest {
//
//    @Test
//    void when_CustomEndpointIsNotProvided_should_CreateDataWriterWithOnlyRegionConfiguration() {
//
//        // Arrange
//        SqsClient mockSqs = mock(SqsClient.class);
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
//        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);
//
////        try (MockedStatic<SqsClientBuilder> utilities = Mockito.mockStatic(SqsClientBuilder.class)) {
//
//            SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
//                    null,
//                    "my-test",
//                    null,
//                    3,
//                    false,
//                    null,
//                    -1,
//                    0,
//                    -1,
//                    -1));
//
//            // Act
//            DataWriter<InternalRow> writer = sut.createWriter(0, 0);
//
//            // Assert
//            assertThat(writer).isNotNull();
//            verify(mockSqsClientBuilder, times(0)).endpointOverride(any(URI.class));
//            verify(mockSqsClientBuilder, times(1)).region(any(Region.class));
////        }
//
//    }
//
//    @Test
//    void when_CustomEndpointIsProvided_should_CreateDataWriterWithEndpointConfiguration() {
//
//        // Arrange
//        SqsClient mockSqs = mock(SqsClient.class);
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//        AmazonSQSClientBuilder mockSqsClientBuilder = mock(AmazonSQSClientBuilder.class);
//        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);
//
//        try (MockedStatic<AmazonSQSClientBuilder> utilities = Mockito.mockStatic(AmazonSQSClientBuilder.class)) {
//            utilities.when(AmazonSQSClientBuilder::standard).thenReturn(mockSqsClientBuilder);
//            SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
//                    "http://host:4566",
//                    "my-test",
//                    null,
//                    3,
//                    SQS,
//                    0,
//                    -1,
//                    -1));
//            // Act
//            DataWriter<InternalRow> writer = sut.createWriter(0, 0);
//
//            // Assert
//            assertThat(writer).isNotNull();
//            verify(mockSqsClientBuilder, times(1)).withEndpointConfiguration(any(AwsClientBuilder.EndpointConfiguration.class));
//        }
//
//    }
//
//    @Test
//    void when_AnotherOwnerAWSAccountIdIsProvided_should_ConfigureUrlRequestWithThisQueueOwnerAWSAccountId() {
//        // Arrange
//        SqsClient mockSqs = mock(SqsClient.class);
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//        AmazonSQSClientBuilder mockSqsClientBuilder = mock(AmazonSQSClientBuilder.class);
//        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);
//
//        try (MockedStatic<AmazonSQSClientBuilder> utilities = Mockito.mockStatic(AmazonSQSClientBuilder.class)) {
//            utilities.when(AmazonSQSClientBuilder::standard).thenReturn(mockSqsClientBuilder);
//            try(MockedConstruction<GetQueueUrlRequest> mockGetQueueUrlRequest = Mockito.mockConstruction(GetQueueUrlRequest.class)) {
//
//                SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
//                        null,
//                        "my-test",
//                        "1234567890",
//                        3,
//                        SQS,
//                        0,
//                        -1,
//                        -1));
//
//                // Act
//                DataWriter<InternalRow> writer = sut.createWriter(0, 0);
//
//                // Assert
//                assertThat(writer).isNotNull();
//                verify(mockGetQueueUrlRequest.constructed().get(0), times(1)).setQueueOwnerAWSAccountId("1234567890");
//            }
//        }
//    }
//
//}