package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SqsSinkDataWriterFactoryUnitTest {

    @Test
    void when_CredentialsProviderIsSystemPropertyCredentialsProvider_should_CreateWriterWithSystemPropertyCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("SystemPropertyCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<SystemPropertyCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(SystemPropertyCredentialsProvider.class)) {

                SystemPropertyCredentialsProvider mockCredentialsProvider = mock(SystemPropertyCredentialsProvider.class);
                staticCredentialsProvider.when(SystemPropertyCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(SystemPropertyCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsEnvironmentVariableCredentialsProvider_should_CreateWriterWithEnvironmentVariableCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("EnvironmentVariableCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<EnvironmentVariableCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(EnvironmentVariableCredentialsProvider.class)) {

                EnvironmentVariableCredentialsProvider mockCredentialsProvider = mock(EnvironmentVariableCredentialsProvider.class);
                staticCredentialsProvider.when(EnvironmentVariableCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(EnvironmentVariableCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsWebIdentityTokenFileCredentialsProvider_should_CreateWriterWithWebIdentityTokenFileCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("WebIdentityTokenFileCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<WebIdentityTokenFileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(WebIdentityTokenFileCredentialsProvider.class)) {

                WebIdentityTokenFileCredentialsProvider mockCredentialsProvider = mock(WebIdentityTokenFileCredentialsProvider.class);
                staticCredentialsProvider.when(WebIdentityTokenFileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(WebIdentityTokenFileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsProfileCredentialsProviderAndProfileIsEmpty_should_CreateWriterWithProfileCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("ProfileCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<ProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ProfileCredentialsProvider.class)) {

                ProfileCredentialsProvider mockCredentialsProvider = mock(ProfileCredentialsProvider.class);
                staticCredentialsProvider.when(ProfileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(ProfileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsProfileCredentialsProviderAndProfileIsNotEmpty_should_CreateWriterWithProfileCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("ProfileCredentialsProvider")
                .profile("localstack")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<ProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ProfileCredentialsProvider.class)) {

                ProfileCredentialsProvider mockCredentialsProvider = mock(ProfileCredentialsProvider.class);
                staticCredentialsProvider.when(() -> ProfileCredentialsProvider.create("localstack")).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(ProfileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsContainerCredentialsProvider_should_CreateWriterWithContainerCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("ContainerCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<ContainerCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ContainerCredentialsProvider.class)) {

                ContainerCredentialsProvider mockCredentialsProvider = mock(ContainerCredentialsProvider.class);
                staticCredentialsProvider.when(ContainerCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(ContainerCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsInstanceProfileCredentialsProvider_should_CreateWriterWithInstanceProfileCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("InstanceProfileCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<InstanceProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(InstanceProfileCredentialsProvider.class)) {

                InstanceProfileCredentialsProvider mockCredentialsProvider = mock(InstanceProfileCredentialsProvider.class);
                staticCredentialsProvider.when(InstanceProfileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(InstanceProfileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsStaticCredentialsProviderAndSessionTokenIsEmpty_should_CreateWriterWithStaticCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("StaticCredentialsProvider")
                .profile("")
                .accessKeyId("test")
                .secretAccessKey("test")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<AwsBasicCredentials> staticAwsCredentials = Mockito.mockStatic(AwsBasicCredentials.class)) {

                AwsBasicCredentials mockAwsCredentials = mock(AwsBasicCredentials.class);
                staticAwsCredentials.when(() -> AwsBasicCredentials.create(anyString(), anyString())).thenReturn(mockAwsCredentials);

                try (MockedStatic<StaticCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(StaticCredentialsProvider.class)) {

                    StaticCredentialsProvider mockCredentialsProvider = mock(StaticCredentialsProvider.class);
                    staticCredentialsProvider.when(() -> StaticCredentialsProvider.create(mockAwsCredentials)).thenReturn(mockCredentialsProvider);

                    SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                    // Act
                    DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                    // Assert
                    assertNotNull(writer);
                    verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(StaticCredentialsProvider.class));

                }

            }

        }

    }

    @Test
    void when_CredentialsProviderIsStaticCredentialsProviderAndSessionTokenIsNotEmpty_should_CreateWriterWithStaticCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("StaticCredentialsProvider")
                .profile("")
                .accessKeyId("test")
                .secretAccessKey("test")
                .sessionToken("test")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<AwsSessionCredentials> staticAwsCredentials = Mockito.mockStatic(AwsSessionCredentials.class)) {

                AwsSessionCredentials mockAwsCredentials = mock(AwsSessionCredentials.class);
                staticAwsCredentials.when(() -> AwsSessionCredentials.create(anyString(), anyString(), anyString())).thenReturn(mockAwsCredentials);

                try (MockedStatic<StaticCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(StaticCredentialsProvider.class)) {

                    StaticCredentialsProvider mockCredentialsProvider = mock(StaticCredentialsProvider.class);
                    staticCredentialsProvider.when(() -> StaticCredentialsProvider.create(mockAwsCredentials)).thenReturn(mockCredentialsProvider);

                    SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                    // Act
                    DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                    // Assert
                    assertNotNull(writer);
                    verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(StaticCredentialsProvider.class));

                }

            }

        }

    }

    @Test
    void when_CredentialsProviderIsAnonymousCredentialsProvider_should_CreateWriterWithAnonymousCredentialsProvider() {

        // Arrange
        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("AnonymousCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<AnonymousCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(AnonymousCredentialsProvider.class)) {

                AnonymousCredentialsProvider mockCredentialsProvider = mock(AnonymousCredentialsProvider.class);
                staticCredentialsProvider.when(AnonymousCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockSqsClientBuilder, times(1)).credentialsProvider(any(AnonymousCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_SqsEndpointIsNotEmpty_should_CreateWriterWithSqsEndpoint() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("DefaultCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

            // Act
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Assert
            assertNotNull(writer);
            verify(mockSqsClientBuilder, times(1)).endpointOverride(URI.create("http://localhost:4566"));

        }

    }

    @Test
    void when_SqsEndpointIsEmpty_should_CreateWriterWithoutSqsEndpoint() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("DefaultCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(false)
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

            // Act
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Assert
            assertNotNull(writer);
            verify(mockSqsClientBuilder, times(0)).endpointOverride(any(URI.class));

        }

    }

//    @Test
//    void when_SqsQueueOwnerAWSAccountIdIsNotEmpty_should_CreateWriterWithQueueOwnerAWSAccountId() {
//
//        String queueOwnerAWSAccountId = "123456789012";
//
//        SqsSinkOptions options = SqsSinkOptions.builder()
//                .credentialsProvider("DefaultCredentialsProvider")
//                .profile("")
//                .accessKeyId("")
//                .secretAccessKey("")
//                .sessionToken("")
//                .endpoint("http://localhost:4566")
//                .region("us-east-1")
//                .queueName("test-queue")
//                .queueOwnerAWSAccountId(queueOwnerAWSAccountId)
//                .batchSize(10)
//                .useSqsExtendedClient(false)
//                .build();
//
//        SqsClient mockSqsClient = mock(SqsClient.class);
//        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//
//        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
//        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);
//
//        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {
//
//            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);
//
//            GetQueueUrlRequest getQueueUrlRequest = mock(GetQueueUrlRequest.class);
//
//            GetQueueUrlRequest.Builder mockGetQueueUrlRequestBuilder = mock(GetQueueUrlRequest.Builder.class);
//            when(mockGetQueueUrlRequestBuilder.build()).thenReturn(getQueueUrlRequest);
//
//            try (MockedStatic<GetQueueUrlRequest> staticGetQueueUrlRequest = Mockito.mockStatic(GetQueueUrlRequest.class)) {
//
//                staticGetQueueUrlRequest.when(GetQueueUrlRequest::builder).thenReturn(mockGetQueueUrlRequestBuilder);
//
//                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);
//
//                // Act
//                DataWriter<InternalRow> writer = factory.createWriter(0, 0);
//
//                // Assert
//                assertNotNull(writer);
//                verify(mockGetQueueUrlRequestBuilder, times(1)).queueOwnerAWSAccountId(queueOwnerAWSAccountId);
//
//            }
//
//        }
//
//    }

//    @Test
//    void testCreateWriterWithExtendedClient() {
//
//        SqsSinkOptions options = SqsSinkOptions.builder()
//                .credentialsProvider("DefaultCredentialsProvider")
//                .profile("")
//                .accessKeyId("")
//                .secretAccessKey("")
//                .sessionToken("")
//                .endpoint("http://localhost:4566")
//                .region("us-east-1")
//                .queueName("test-queue")
//                .queueOwnerAWSAccountId("123456789012")
//                .batchSize(10)
//                .useSqsExtendedClient(true)
//                .s3credentialsProvider("DefaultCredentialsProvider")
//                .s3profile("")
//                .s3accessKeyId("")
//                .s3secretAccessKey("")
//                .s3sessionToken("")
//                .s3Endpoint("")
//                .s3Endpoint("")
//                .s3Region("us-east-1")
//                .bucketName("")
//                .payloadSizeThreshold(-1)
//                .build();
//
//        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);
//
//        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
//            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
//            SqsClient mockClient = mock(SqsClient.class);
//            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);
//
//            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
//            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
//            when(mockBuilder.build()).thenReturn(mockClient);
//            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
//            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");
//
//            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);
//
//            DataWriter<InternalRow> writer = factory.createWriter(0, 0);
//
//            assertNotNull(writer);
//            assertInstanceOf(SqsSinkDataWriter.class, writer);
//        }
//
//    }

    @Test
    void when_S3EndpointIsNotEmpty_should_CreateWriterWithS3Endpoint() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .credentialsProvider("DefaultCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("")
                .batchSize(10)
                .useSqsExtendedClient(true)
                .s3CredentialsProvider("DefaultCredentialsProvider")
                .s3Profile("")
                .s3AccessKeyId("")
                .s3SecretAccessKey("")
                .s3SessionToken("")
                .s3Endpoint("")
                .s3Endpoint("http://localhost:4566")
                .s3Region("us-east-1")
                .bucketName("test-bucket")
                .s3KeyPrefix("")
                .build();

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            S3Client mockS3Client = mock(S3Client.class);

            S3ClientBuilder mockS3ClientBuilder = mock(S3ClientBuilder.class);
            when(mockS3ClientBuilder.build()).thenReturn(mockS3Client);

            try (MockedStatic<S3Client> staticS3Client = Mockito.mockStatic(S3Client.class)) {

                staticS3Client.when(S3Client::builder).thenReturn(mockS3ClientBuilder);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockS3ClientBuilder, times(1)).endpointOverride(URI.create("http://localhost:4566"));

            }

        }

    }

//    @Test
//    void when_BucketNameIsNotEmpty_should_CreateWriterWithPayloadSupportEnabled() {
//
//        SqsSinkOptions options = SqsSinkOptions.builder()
//                .credentialsProvider("DefaultCredentialsProvider")
//                .profile("")
//                .accessKeyId("")
//                .secretAccessKey("")
//                .sessionToken("")
//                .endpoint("")
//                .region("us-east-1")
//                .queueName("test-queue")
//                .queueOwnerAWSAccountId("")
//                .batchSize(10)
//                .useSqsExtendedClient(true)
//                .s3credentialsProvider("DefaultCredentialsProvider")
//                .s3profile("")
//                .s3accessKeyId("")
//                .s3secretAccessKey("")
//                .s3sessionToken("")
//                .s3Endpoint("")
//                .s3Endpoint("")
//                .s3Region("us-east-1")
//                .bucketName("test-bucket")
//                .build();
//
//        SqsClient mockSqsClient = mock(SqsClient.class);
//        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//
//        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
//        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);
//
//        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {
//
//            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);
//
//            S3Client mockS3Client = mock(S3Client.class);
//
//            S3ClientBuilder mockS3ClientBuilder = mock(S3ClientBuilder.class);
//            when(mockS3ClientBuilder.build()).thenReturn(mockS3Client);
//
//            try (MockedStatic<S3Client> staticS3Client = Mockito.mockStatic(S3Client.class)) {
//
//                staticS3Client.when(S3Client::builder).thenReturn(mockS3ClientBuilder);
//
//                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);
//
//                // Act
//                SqsSinkDataWriter writer = (SqsSinkDataWriter) factory.createWriter(0, 0);
//
//                // Assert
//                assertNotNull(writer);
//                verify(mockS3ClientBuilder, times(1)).endpointOverride(URI.create("http://localhost:4566"));
//
//            }
//
//        }
//
//    }
//
//    @Test
//    void testCreateWriterWithPayloadSizeThreshold() {
//
//        SqsSinkOptions options = SqsSinkOptions.builder()
//                .credentialsProvider("DefaultCredentialsProvider")
//                .profile("")
//                .accessKeyId("")
//                .secretAccessKey("")
//                .sessionToken("")
//                .endpoint("http://localhost:4566")
//                .region("us-east-1")
//                .queueName("test-queue")
//                .queueOwnerAWSAccountId("123456789012")
//                .batchSize(10)
//                .useSqsExtendedClient(true)
//                .s3credentialsProvider("DefaultCredentialsProvider")
//                .s3profile("")
//                .s3accessKeyId("")
//                .s3secretAccessKey("")
//                .s3sessionToken("")
//                .s3Endpoint("")
//                .s3Region("us-east-1")
//                .bucketName("test-bucket")
//                .payloadSizeThreshold(256)
//                .build();
//
//        SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(options);
//
//        try (MockedStatic<SqsClient> mockedSqsClient = Mockito.mockStatic(SqsClient.class)) {
//            SqsClientBuilder mockBuilder = mock(SqsClientBuilder.class);
//            SqsClient mockClient = mock(SqsClient.class);
//            GetQueueUrlResponse mockResponse = mock(GetQueueUrlResponse.class);
//
//            when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
//            when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
//            when(mockBuilder.build()).thenReturn(mockClient);
//            when(mockClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(mockResponse);
//            when(mockResponse.queueUrl()).thenReturn("http://localhost:4566/123456789012/test-queue");
//
//            mockedSqsClient.when(SqsClient::builder).thenReturn(mockBuilder);
//
//            DataWriter<InternalRow> writer = factory.createWriter(0, 0);
//
//            assertNotNull(writer);
//            assertInstanceOf(SqsSinkDataWriter.class, writer);
//        }
//
//    }

}