package com.leonardozv.spark.connectors.aws.dynamodb.write;

import java.io.Serializable;
import java.util.Set;

public class DynamoDbSinkOptions implements Serializable {

    private final String endpoint;
    private final String region;
    private final int batchSize;
    private final Set<String> errorsToIgnore;
    private final int statementColumnIndex;

    public DynamoDbSinkOptions(Builder builder) {
        this.endpoint = builder.endpoint;
        this.region = builder.region;
        this.batchSize = builder.batchSize;
        this.errorsToIgnore = builder.errorsToIgnore;
        this.statementColumnIndex = builder.statementColumnIndex;
    }

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

        private String endpoint;
        private String region;
        private int batchSize;
        private Set<String> errorsToIgnore;
        private int statementColumnIndex;

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
