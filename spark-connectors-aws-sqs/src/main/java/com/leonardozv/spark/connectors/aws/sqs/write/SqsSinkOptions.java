package com.leonardozv.spark.connectors.aws.sqs.write;

import java.io.Serializable;

public class SqsSinkOptions implements Serializable {

    private final String credentialsProvider;
    private final String profile;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final String endpoint;
    private final String region;
    private final String queueName;
    private final String queueOwnerAWSAccountId;
    private final int batchSize;
    private final boolean useSqsExtendedClient;
    private final String s3CredentialsProvider;
    private final String s3Profile;
    private final String s3AccessKeyId;
    private final String s3SecretAccessKey;
    private final String s3SessionToken;
    private final String s3Endpoint;
    private final String s3Region;
    private final boolean forcePathStyle;
    private final String bucketName;
    private final String s3KeyPrefix;
    private final int payloadSizeThreshold;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SqsSinkOptions(Builder builder) {
        this.credentialsProvider = builder.credentialsProvider;
        this.profile = builder.profile;
        this.accessKeyId = builder.accessKeyId;
        this.secretAccessKey = builder.secretAccessKey;
        this.sessionToken = builder.sessionToken;
        this.endpoint = builder.endpoint;
        this.region = builder.region;
        this.queueName = builder.queueName;
        this.queueOwnerAWSAccountId = builder.queueOwnerAWSAccountId;
        this.batchSize = builder.batchSize;
        this.useSqsExtendedClient = builder.useSqsExtendedClient;
        this.s3CredentialsProvider = builder.s3CredentialsProvider;
        this.s3Profile = builder.s3Profile;
        this.s3AccessKeyId = builder.s3AccessKeyId;
        this.s3SecretAccessKey = builder.s3SecretAccessKey;
        this.s3SessionToken = builder.s3SessionToken;
        this.s3Endpoint = builder.s3Endpoint;
        this.s3Region = builder.s3Region;
        this.forcePathStyle = builder.forcePathStyle;
        this.bucketName = builder.bucketName;
        this.payloadSizeThreshold = builder.payloadSizeThreshold;
        this.s3KeyPrefix = builder.s3KeyPrefix;
        this.valueColumnIndex = builder.valueColumnIndex;
        this.msgAttributesColumnIndex = builder.msgAttributesColumnIndex;
        this.groupIdColumnIndex = builder.groupIdColumnIndex;
    }

    public String credentialsProvider() { return credentialsProvider; }
    public String profile() { return profile; }
    public String accessKeyId() { return accessKeyId; }
    public String secretAccessKey() { return secretAccessKey; }
    public String sessionToken() { return sessionToken; }
    public String endpoint() { return endpoint; }
    public String region() { return region; }
    public String queueName() { return queueName; }
    public String queueOwnerAWSAccountId() { return queueOwnerAWSAccountId; }
    public int batchSize() { return batchSize; }
    public boolean useSqsExtendedClient() { return useSqsExtendedClient; }
    public String s3CredentialsProvider() { return s3CredentialsProvider; }
    public String s3Profile() { return s3Profile; }
    public String s3AccessKeyId() { return s3AccessKeyId; }
    public String s3SecretAccessKey() { return s3SecretAccessKey; }
    public String s3SessionToken() { return s3SessionToken; }
    public String s3Endpoint() { return s3Endpoint; }
    public String s3Region() { return s3Region; }
    public boolean forcePathStyle() { return forcePathStyle; }
    public String bucketName() { return bucketName; }
    public int payloadSizeThreshold() { return payloadSizeThreshold; }
    public String s3KeyPrefix() { return s3KeyPrefix; }
    public int valueColumnIndex() { return valueColumnIndex; }
    public int msgAttributesColumnIndex() { return msgAttributesColumnIndex; }
    public int groupIdColumnIndex() { return groupIdColumnIndex; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements Serializable {

        private String credentialsProvider;
        private String profile;
        private String accessKeyId;
        private String secretAccessKey;
        private String sessionToken;
        private String endpoint;
        private String region;
        private String queueName;
        private String queueOwnerAWSAccountId;
        private int batchSize;
        private boolean useSqsExtendedClient;
        private String s3CredentialsProvider;
        private String s3Profile;
        private String s3AccessKeyId;
        private String s3SecretAccessKey;
        private String s3SessionToken;
        private String s3Endpoint;
        private String s3Region;
        private boolean forcePathStyle;
        private String bucketName;
        private int payloadSizeThreshold;
        private String s3KeyPrefix;
        private int valueColumnIndex;
        private int msgAttributesColumnIndex;
        private int groupIdColumnIndex;

        public Builder credentialsProvider(String credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public Builder profile(String profile) {
            this.profile = profile;
            return this;
        }

        public Builder accessKeyId(String accessKeyId) {
            this.accessKeyId = accessKeyId;
            return this;
        }

        public Builder secretAccessKey(String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        public Builder sessionToken(String sessionToken) {
            this.sessionToken = sessionToken;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder queueOwnerAWSAccountId(String queueOwnerAWSAccountId) {
            this.queueOwnerAWSAccountId = queueOwnerAWSAccountId;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder useSqsExtendedClient(boolean useSqsExtendedClient) {
            this.useSqsExtendedClient = useSqsExtendedClient;
            return this;
        }

        public Builder s3CredentialsProvider(String s3CredentialsProvider) {
            this.s3CredentialsProvider = s3CredentialsProvider;
            return this;
        }

        public Builder s3Profile(String s3Profile) {
            this.s3Profile = s3Profile;
            return this;
        }

        public Builder s3AccessKeyId(String s3AccessKeyId) {
            this.s3AccessKeyId = s3AccessKeyId;
            return this;
        }

        public Builder s3SecretAccessKey(String s3SecretAccessKey) {
            this.s3SecretAccessKey = s3SecretAccessKey;
            return this;
        }

        public Builder s3SessionToken(String s3SessionToken) {
            this.s3SessionToken = s3SessionToken;
            return this;
        }

        public Builder s3Endpoint(String s3Endpoint) {
            this.s3Endpoint = s3Endpoint;
            return this;
        }

        public Builder s3Region(String s3Region) {
            this.s3Region = s3Region;
            return this;
        }

        public Builder forcePathStyle(boolean forcePathStyle) {
            this.forcePathStyle = forcePathStyle;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder payloadSizeThreshold(int payloadSizeThreshold) {
            this.payloadSizeThreshold = payloadSizeThreshold;
            return this;
        }

        public Builder s3KeyPrefix(String s3KeyPrefix) {
            this.s3KeyPrefix = s3KeyPrefix;
            return this;
        }

        public Builder valueColumnIndex(int valueColumnIndex) {
            this.valueColumnIndex = valueColumnIndex;
            return this;
        }

        public Builder msgAttributesColumnIndex(int msgAttributesColumnIndex) {
            this.msgAttributesColumnIndex = msgAttributesColumnIndex;
            return this;
        }

        public Builder groupIdColumnIndex(int groupIdColumnIndex) {
            this.groupIdColumnIndex = groupIdColumnIndex;
            return this;
        }

        public SqsSinkOptions build() {
            return new SqsSinkOptions(this);
        }

    }

}