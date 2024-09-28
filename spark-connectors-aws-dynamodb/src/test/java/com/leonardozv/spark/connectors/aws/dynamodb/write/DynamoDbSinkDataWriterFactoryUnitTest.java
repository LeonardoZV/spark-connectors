package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoDbSinkDataWriterFactoryUnitTest {

    @Test
    void when_CredentialsProviderIsSystemPropertyCredentialsProvider_should_CreateWriterWithSystemPropertyCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("SystemPropertyCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<SystemPropertyCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(SystemPropertyCredentialsProvider.class)) {

                SystemPropertyCredentialsProvider mockCredentialsProvider = mock(SystemPropertyCredentialsProvider.class);
                staticCredentialsProvider.when(SystemPropertyCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(SystemPropertyCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsEnvironmentVariableCredentialsProvider_should_CreateWriterWithEnvironmentVariableCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("EnvironmentVariableCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<EnvironmentVariableCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(EnvironmentVariableCredentialsProvider.class)) {

                EnvironmentVariableCredentialsProvider mockCredentialsProvider = mock(EnvironmentVariableCredentialsProvider.class);
                staticCredentialsProvider.when(EnvironmentVariableCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(EnvironmentVariableCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsWebIdentityTokenFileCredentialsProvider_should_CreateWriterWithWebIdentityTokenFileCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("WebIdentityTokenFileCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<WebIdentityTokenFileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(WebIdentityTokenFileCredentialsProvider.class)) {

                WebIdentityTokenFileCredentialsProvider mockCredentialsProvider = mock(WebIdentityTokenFileCredentialsProvider.class);
                staticCredentialsProvider.when(WebIdentityTokenFileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(WebIdentityTokenFileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsProfileCredentialsProviderAndProfileIsEmpty_should_CreateWriterWithProfileCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("ProfileCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<ProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ProfileCredentialsProvider.class)) {

                ProfileCredentialsProvider mockCredentialsProvider = mock(ProfileCredentialsProvider.class);
                staticCredentialsProvider.when(ProfileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(ProfileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsProfileCredentialsProviderAndProfileIsNotEmpty_should_CreateWriterWithProfileCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("ProfileCredentialsProvider")
                .profile("localstack")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<ProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ProfileCredentialsProvider.class)) {

                ProfileCredentialsProvider mockCredentialsProvider = mock(ProfileCredentialsProvider.class);
                staticCredentialsProvider.when(() -> ProfileCredentialsProvider.create("localstack")).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(ProfileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsContainerCredentialsProvider_should_CreateWriterWithContainerCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("ContainerCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<ContainerCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(ContainerCredentialsProvider.class)) {

                ContainerCredentialsProvider mockCredentialsProvider = mock(ContainerCredentialsProvider.class);
                staticCredentialsProvider.when(ContainerCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(ContainerCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsInstanceProfileCredentialsProvider_should_CreateWriterWithInstanceProfileCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("InstanceProfileCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<InstanceProfileCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(InstanceProfileCredentialsProvider.class)) {

                InstanceProfileCredentialsProvider mockCredentialsProvider = mock(InstanceProfileCredentialsProvider.class);
                staticCredentialsProvider.when(InstanceProfileCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(InstanceProfileCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_CredentialsProviderIsStaticCredentialsProviderAndSessionTokenIsEmpty_should_CreateWriterWithStaticCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("StaticCredentialsProvider")
                .profile("")
                .accessKeyId("test")
                .secretAccessKey("test")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<AwsBasicCredentials> staticAwsCredentials = Mockito.mockStatic(AwsBasicCredentials.class)) {

                AwsBasicCredentials mockAwsCredentials = mock(AwsBasicCredentials.class);
                staticAwsCredentials.when(() -> AwsBasicCredentials.create(anyString(), anyString())).thenReturn(mockAwsCredentials);

                try (MockedStatic<StaticCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(StaticCredentialsProvider.class)) {

                    StaticCredentialsProvider mockCredentialsProvider = mock(StaticCredentialsProvider.class);
                    staticCredentialsProvider.when(() -> StaticCredentialsProvider.create(mockAwsCredentials)).thenReturn(mockCredentialsProvider);

                    DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                    // Act
                    DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                    // Assert
                    assertNotNull(writer);
                    verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(StaticCredentialsProvider.class));

                }

            }

        }

    }

    @Test
    void when_CredentialsProviderIsStaticCredentialsProviderAndSessionTokenIsNotEmpty_should_CreateWriterWithStaticCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("StaticCredentialsProvider")
                .profile("")
                .accessKeyId("test")
                .secretAccessKey("test")
                .sessionToken("test")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<AwsSessionCredentials> staticAwsCredentials = Mockito.mockStatic(AwsSessionCredentials.class)) {

                AwsSessionCredentials mockAwsCredentials = mock(AwsSessionCredentials.class);
                staticAwsCredentials.when(() -> AwsSessionCredentials.create(anyString(), anyString(), anyString())).thenReturn(mockAwsCredentials);

                try (MockedStatic<StaticCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(StaticCredentialsProvider.class)) {

                    StaticCredentialsProvider mockCredentialsProvider = mock(StaticCredentialsProvider.class);
                    staticCredentialsProvider.when(() -> StaticCredentialsProvider.create(mockAwsCredentials)).thenReturn(mockCredentialsProvider);

                    DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                    // Act
                    DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                    // Assert
                    assertNotNull(writer);
                    verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(StaticCredentialsProvider.class));

                }

            }

        }

    }

    @Test
    void when_CredentialsProviderIsAnonymousCredentialsProvider_should_CreateWriterWithAnonymousCredentialsProvider() {

        // Arrange
        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("AnonymousCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            try (MockedStatic<AnonymousCredentialsProvider> staticCredentialsProvider = Mockito.mockStatic(AnonymousCredentialsProvider.class)) {

                AnonymousCredentialsProvider mockCredentialsProvider = mock(AnonymousCredentialsProvider.class);
                staticCredentialsProvider.when(AnonymousCredentialsProvider::create).thenReturn(mockCredentialsProvider);

                DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

                // Act
                DataWriter<InternalRow> writer = factory.createWriter(0, 0);

                // Assert
                assertNotNull(writer);
                verify(mockDynamoDbClientBuilder, times(1)).credentialsProvider(any(AnonymousCredentialsProvider.class));

            }

        }

    }

    @Test
    void when_EndpointIsNotEmpty_should_CreateWriterWithEndpoint() {

        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("DefaultCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("http://localhost:8000")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

            // Act
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Assert
            assertNotNull(writer);
            verify(mockDynamoDbClientBuilder, times(1)).endpointOverride(URI.create("http://localhost:8000"));

        }

    }

    @Test
    void when_EndpointIsEmpty_should_CreateWriterWithoutEndpoint() {

        DynamoDbSinkOptions options = new DynamoDbSinkOptions.Builder()
                .credentialsProvider("DefaultCredentialsProvider")
                .profile("")
                .accessKeyId("")
                .secretAccessKey("")
                .sessionToken("")
                .endpoint("")
                .region("us-west-2")
                .batchSize(25)
                .errorsToIgnore(new HashSet<>())
                .statementColumnIndex(0)
                .build();

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbClientBuilder mockDynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
        when(mockDynamoDbClientBuilder.build()).thenReturn(mockDynamoDbClient);

        try (MockedStatic<DynamoDbClient> staticDynamoDbClient = Mockito.mockStatic(DynamoDbClient.class)) {

            staticDynamoDbClient.when(DynamoDbClient::builder).thenReturn(mockDynamoDbClientBuilder);

            DynamoDbSinkDataWriterFactory factory = new DynamoDbSinkDataWriterFactory(options);

            // Act
            DataWriter<InternalRow> writer = factory.createWriter(0, 0);

            // Assert
            assertNotNull(writer);
            verify(mockDynamoDbClientBuilder, times(0)).endpointOverride(any(URI.class));

        }

    }

}
