package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;

public class DynamoDbSinkDataWriterFactory implements DataWriterFactory {

    private final DynamoDbSinkOptions options;
    private final StructType schema;

    public DynamoDbSinkDataWriterFactory(DynamoDbSinkOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {

        DynamoDbClient dynamoDbClient = getDynamoDbClient();

        return new DynamoDbSinkDataWriter(partitionId, taskId, dynamoDbClient, this.options, this.schema);

    }

    private AwsCredentialsProvider identifyCredentialsProvider(String credentialsProvider, String profile, String accessKeyId, String secretAccessKey, String sessionToken) {

        switch (credentialsProvider) {

            case "SystemPropertyCredentialsProvider":
                return SystemPropertyCredentialsProvider.create();

            case "EnvironmentVariableCredentialsProvider":
                return EnvironmentVariableCredentialsProvider.create();

            case "WebIdentityTokenFileCredentialsProvider":
                return WebIdentityTokenFileCredentialsProvider.create();

            case "ProfileCredentialsProvider":
                if (profile.isEmpty())
                    return ProfileCredentialsProvider.create();
                else
                    return ProfileCredentialsProvider.create(profile);

            case "ContainerCredentialsProvider":
                return ContainerCredentialsProvider.create();

            case "InstanceProfileCredentialsProvider":
                return InstanceProfileCredentialsProvider.create();

            case "StaticCredentialsProvider":
                AwsCredentials credentials;
                if (sessionToken.isEmpty())
                    credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
                else
                    credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
                return StaticCredentialsProvider.create(credentials);

            case "AnonymousCredentialsProvider":
                return AnonymousCredentialsProvider.create();

            default:
                return DefaultCredentialsProvider.create();

        }

    }

    private DynamoDbClient getDynamoDbClient() {

        DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();

        clientBuilder.credentialsProvider(identifyCredentialsProvider(this.options.credentialsProvider(), this.options.profile(), this.options.accessKeyId(), this.options.secretAccessKey(), this.options.sessionToken()));

        clientBuilder.region(this.options.region());

        if (!this.options.endpoint().isEmpty())
            clientBuilder.endpointOverride(URI.create(this.options.endpoint()));

        return clientBuilder.build();

    }

}
