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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SqsSinkDataWriterFactoryUnitTest {

    @Test
    void when_CredentialsProviderIsSystemPropertyCredentialsProvider_should_CreateWriterWithSystemPropertyCredentialsProvider() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "SystemPropertyCredentialsProvider");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<SystemPropertyCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(SystemPropertyCredentialsProvider.class)) {

                SystemPropertyCredentialsProvider mockCredentialsProvider = mock(SystemPropertyCredentialsProvider.class);
                staticCredentialsProvider.when(SystemPropertyCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "EnvironmentVariableCredentialsProvider");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<EnvironmentVariableCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(EnvironmentVariableCredentialsProvider.class)) {

                EnvironmentVariableCredentialsProvider mockCredentialsProvider = mock(EnvironmentVariableCredentialsProvider.class);
                staticCredentialsProvider.when(EnvironmentVariableCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "WebIdentityTokenFileCredentialsProvider");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<WebIdentityTokenFileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(WebIdentityTokenFileCredentialsProvider.class)) {

                WebIdentityTokenFileCredentialsProvider mockCredentialsProvider = mock(WebIdentityTokenFileCredentialsProvider.class);
                staticCredentialsProvider.when(WebIdentityTokenFileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "ProfileCredentialsProvider");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<ProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ProfileCredentialsProvider.class)) {

                ProfileCredentialsProvider mockCredentialsProvider = mock(ProfileCredentialsProvider.class);
                staticCredentialsProvider.when(ProfileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "ProfileCredentialsProvider");
            put("profile", "localstack");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<ProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ProfileCredentialsProvider.class)) {

                ProfileCredentialsProvider mockCredentialsProvider = mock(ProfileCredentialsProvider.class);
                staticCredentialsProvider.when(() -> ProfileCredentialsProvider.create("localstack")).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "ContainerCredentialsProvider");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<ContainerCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ContainerCredentialsProvider.class)) {

                ContainerCredentialsProvider mockCredentialsProvider = mock(ContainerCredentialsProvider.class);
                staticCredentialsProvider.when(ContainerCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "InstanceProfileCredentialsProvider");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<InstanceProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(InstanceProfileCredentialsProvider.class)) {

                InstanceProfileCredentialsProvider mockCredentialsProvider = mock(InstanceProfileCredentialsProvider.class);
                staticCredentialsProvider.when(InstanceProfileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "StaticCredentialsProvider");
            put("accessKeyId", "test");
            put("secretAccessKey", "test");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

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

                    SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "StaticCredentialsProvider");
            put("accessKeyId", "test");
            put("secretAccessKey", "test");
            put("sessionToken", "test");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

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

                    SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("credentialsProvider", "AnonymousCredentialsProvider");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            try (MockedStatic<AnonymousCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(AnonymousCredentialsProvider.class)) {

                AnonymousCredentialsProvider mockCredentialsProvider = mock(AnonymousCredentialsProvider.class);
                staticCredentialsProvider.when(AnonymousCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

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

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:4566");
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

            // Act
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Assert
            assertNotNull(writer);
            verify(mockSqsClientBuilder, times(1)).endpointOverride(URI.create("http://localhost:4566"));

        }

    }

    @Test
    void when_SqsEndpointIsEmpty_should_CreateWriterWithoutSqsEndpoint() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("region", "us-east-1");
            put("queueName", "test-queue");
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

            // Act
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Assert
            assertNotNull(writer);
            verify(mockSqsClientBuilder, times(0)).endpointOverride(any(URI.class));

        }

    }

    @Test
    void when_SqsQueueOwnerAWSAccountIdIsNotEmpty_should_CreateWriterWithQueueOwnerAWSAccountId() {

        String queueOwnerAWSAccountId = "123456789012";

        HashMap<String, String> options = new HashMap<String, String>() {{
            put("region", "us-east-1");
            put("queueName", "test-queue");
            put("queueOwnerAWSAccountId", queueOwnerAWSAccountId);
        }};

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());

        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqsClient);

        try (MockedStatic<SqsClient> staticSqsClient = Mockito.mockStatic(SqsClient.class)) {

            staticSqsClient.when(SqsClient::builder).thenReturn(mockSqsClientBuilder);

            GetQueueUrlRequest getQueueUrlRequest = mock(GetQueueUrlRequest.class);

            GetQueueUrlRequest.Builder mockGetQueueUrlRequestBuilder = mock(GetQueueUrlRequest.Builder.class);
            when(mockGetQueueUrlRequestBuilder.queueName(anyString())).thenReturn(mockGetQueueUrlRequestBuilder);
            when(mockGetQueueUrlRequestBuilder.build()).thenReturn(getQueueUrlRequest);

            try (MockedStatic<GetQueueUrlRequest> staticGetQueueUrlRequest = Mockito.mockStatic(GetQueueUrlRequest.class)) {

                staticGetQueueUrlRequest.when(GetQueueUrlRequest::builder).thenReturn(mockGetQueueUrlRequestBuilder);

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockGetQueueUrlRequestBuilder, times(1)).queueOwnerAWSAccountId(queueOwnerAWSAccountId);

            }

        }

    }

    @Test
    void testCreateWriterWithExtendedClient() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("region", "us-east-1");
            put("queueName", "test-queue");
            put("useSqsExtendedClient", "true");
            put("s3Endpoint", "http://localhost:4566");
            put("s3Region", "us-east-1");
            put("bucketName", "test-bucket");
            put("payloadSizeThreshold", "1");
            put("s3KeyPrefix", "test/");
        }};

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

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

                // Act
                SqsSinkDataWriter writer = (SqsSinkDataWriter) factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockS3ClientBuilder, times(1)).endpointOverride(URI.create("http://localhost:4566"));

            }

        }

    }

    @Test
    void when_S3EndpointIsEmpty_should_CreateWriterWithoutS3Endpoint() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("region", "us-east-1");
            put("queueName", "test-queue");
            put("useSqsExtendedClient", "true");
            put("s3Region", "us-east-1");
            put("bucketName", "test-bucket");
        }};

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

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

                // Act
                SqsSinkDataWriter writer = (SqsSinkDataWriter) factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockS3ClientBuilder, times(0)).endpointOverride(any(URI.class));

            }

        }

    }

    @Test
    void when_BucketNameIsEmpty_should_CreateWriterWithoutPayloadSupportEnabled() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("region", "us-east-1");
            put("queueName", "test-queue");
            put("useSqsExtendedClient", "true");
            put("s3Region", "us-east-1");
        }};

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

                SqsSinkDataWriterFactory factory = new SqsSinkDataWriterFactory(new SqsSinkOptions(options), 0, 0, 0);

                // Act
                SqsSinkDataWriter writer = (SqsSinkDataWriter) factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockS3ClientBuilder, times(0)).endpointOverride(any(URI.class));

            }

        }

    }

}