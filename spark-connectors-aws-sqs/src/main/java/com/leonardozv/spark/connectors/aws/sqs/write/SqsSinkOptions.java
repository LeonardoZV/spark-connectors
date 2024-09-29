package com.leonardozv.spark.connectors.aws.sqs.write;

import software.amazon.awssdk.regions.Region;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SqsSinkOptions implements Serializable {

    private final Map<String, String> options;

    public SqsSinkOptions(Map<String, String> options) {
        this.options = new HashMap<>(options);
    }

    public String credentialsProvider() {
        return this.options.computeIfAbsent("credentialsProvider", k -> "DefaultCredentialsProvider");
    }

    public String profile() {
        return this.options.computeIfAbsent("profile", k -> "");
    }

    public String accessKeyId() {
        return this.options.computeIfAbsent("accessKeyId", k -> "");
    }

    public String secretAccessKey() {
        return this.options.computeIfAbsent("secretAccessKey", k -> "");
    }

    public String sessionToken() {
        return this.options.computeIfAbsent("sessionToken", k -> "");
    }

    public String endpoint() {
        return this.options.computeIfAbsent("endpoint", k -> "");
    }

    public Region region() {
        return Region.of(this.options.computeIfAbsent("region", k -> "us-east-1"));
    }

    public String queueName() {
        return this.options.computeIfAbsent("queueName", k -> "");
    }

    public String queueOwnerAWSAccountId() {
        return this.options.computeIfAbsent("queueOwnerAWSAccountId", k -> "");
    }

    public int batchSize() {
        return Integer.parseInt(this.options.computeIfAbsent("batchSize", k -> "10"));
    }

    public boolean useSqsExtendedClient() {
        return Boolean.parseBoolean(this.options.computeIfAbsent("useSqsExtendedClient", k -> "false"));
    }

    public String s3CredentialsProvider() {
        return this.options.computeIfAbsent("s3CredentialsProvider", k -> "DefaultCredentialsProvider");
    }

    public String s3Profile() {
        return this.options.computeIfAbsent("s3Profile", k -> "");
    }

    public String s3AccessKeyId() {
        return this.options.computeIfAbsent("s3AccessKeyId", k -> "");
    }

    public String s3SecretAccessKey() {
        return this.options.computeIfAbsent("s3SecretAccessKey", k -> "");
    }

    public String s3SessionToken() {
        return this.options.computeIfAbsent("s3SessionToken", k -> "");
    }

    public String s3Endpoint() {
        return this.options.computeIfAbsent("s3Endpoint", k -> "");
    }

    public Region s3Region() {
        return Region.of(this.options.computeIfAbsent("s3Region", k -> "us-east-1"));
    }

    public boolean forcePathStyle() {
        return Boolean.parseBoolean(this.options.computeIfAbsent("forcePathStyle", k -> "false"));
    }

    public String bucketName() {
        return this.options.computeIfAbsent("bucketName", k -> "");
    }

    public int payloadSizeThreshold() {
        return Integer.parseInt(this.options.computeIfAbsent("payloadSizeThreshold", k -> "-1"));
    }

    public String s3KeyPrefix() {
        return this.options.computeIfAbsent("s3KeyPrefix", k -> "");
    }

}