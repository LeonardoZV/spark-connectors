package com.leonardozv.spark.connectors.aws.dynamodb;

import java.io.Serializable;

public class DynamoDbSinkOptions implements Serializable {

    private final String region;
    private final String endpoint;
    private final int batchSize;
    private final boolean ignoreConditionalCheckFailedError;
    private final int statementColumnIndex;

    public DynamoDbSinkOptions(String region,
                               String endpoint,
                               int batchSize,
                               boolean ignoreConditionalCheckFailedError,
                               int statementColumnIndex) {
        this.region = region != null ? region : "us-east-1";
        this.endpoint = endpoint != null ? endpoint : "";
        this.batchSize = batchSize;
        this.ignoreConditionalCheckFailedError = ignoreConditionalCheckFailedError;
        this.statementColumnIndex = statementColumnIndex;
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

    public boolean ignoreConditionalCheckFailedError() {
        return ignoreConditionalCheckFailedError;
    }

    public int statementColumnIndex() {
        return statementColumnIndex;
    }

}
