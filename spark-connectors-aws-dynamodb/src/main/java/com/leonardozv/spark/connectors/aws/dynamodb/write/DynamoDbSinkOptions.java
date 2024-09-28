package com.leonardozv.spark.connectors.aws.dynamodb.write;

import java.io.Serializable;
import java.util.Set;

public class DynamoDbSinkOptions implements Serializable {

    private final String credentialsProvider;
    private final String profile;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final String endpoint;
    private final String region;
    private final int batchSize;
    private final Set<String> errorsToIgnore;
    private final int statementColumnIndex;

    public DynamoDbSinkOptions(Builder builder) {
        this.credentialsProvider = builder.credentialsProvider;
        this.profile = builder.profile;
        this.accessKeyId = builder.accessKeyId;
        this.secretAccessKey = builder.secretAccessKey;
        this.sessionToken = builder.sessionToken;
        this.endpoint = builder.endpoint;
        this.region = builder.region;
        this.batchSize = builder.batchSize;
        this.errorsToIgnore = builder.errorsToIgnore;
        this.statementColumnIndex = builder.statementColumnIndex;
    }

    public String credentialsProvider() { return credentialsProvider; }
    public String profile() { return profile; }
    public String accessKeyId() { return accessKeyId; }
    public String secretAccessKey() { return secretAccessKey; }
    public String sessionToken() { return sessionToken; }
    public String endpoint() { return endpoint; }
    public String region() {
        return region;
    }
    public int batchSize() {
        return batchSize;
    }
    public Set<String> errorsToIgnore() {
        return errorsToIgnore;
    }
    public int statementColumnIndex() {
        return statementColumnIndex;
    }

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
        private int batchSize;
        private Set<String> errorsToIgnore;
        private int statementColumnIndex;

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

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder errorsToIgnore(Set<String> errorsToIgnore) {
            this.errorsToIgnore = errorsToIgnore;
            return this;
        }

        public Builder statementColumnIndex(int statementColumnIndex) {
            this.statementColumnIndex = statementColumnIndex;
            return this;
        }

        public DynamoDbSinkOptions build() {
            return new DynamoDbSinkOptions(this);
        }

    }

}
