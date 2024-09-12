package com.leonardozv.spark.connectors.aws.dynamodb.write;

import java.io.Serializable;
import java.util.Map;

public class DynamoDbSinkOptions implements Serializable {

    private final String region;
    private final String endpoint;
    private final int batchSize;
    private final Map<String, String> errorsToIgnore;
    private final int statementColumnIndex;

    public DynamoDbSinkOptions(Builder builder) {
        this.region = builder.region;
        this.endpoint = builder.endpoint;
        this.batchSize = builder.batchSize;
        this.errorsToIgnore = builder.errorsToIgnore;
        this.statementColumnIndex = builder.statementColumnIndex;
    }

    public String region() {
        return region;
    }
    public String endpoint() {
        return endpoint;
    }
    public int batchSize() {
        return batchSize;
    }
    public Map<String, String> errorsToIgnore() {
        return errorsToIgnore;
    }
    public int statementColumnIndex() {
        return statementColumnIndex;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements Serializable {

        private String region;
        private String endpoint;
        private int batchSize;
        private Map<String, String> errorsToIgnore;
        private int statementColumnIndex;

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder errorsToIgnore(Map<String, String> errorsToIgnore) {
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
