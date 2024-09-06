package com.leonardozv.spark.connectors.aws.dynamodb;

import java.io.Serializable;

public class DynamoDbSinkOptions implements Serializable {

    private final String region;
    private final String endpoint;
    private final int batchSize;
    private final boolean treatConditionalCheckFailedAsError;
    private final int statementColumnIndex;

    public DynamoDbSinkOptions(String region,
                               String endpoint,
                               int batchSize,
                               boolean treatConditionalCheckFailedAsError,
                               int statementColumnIndex) {
        this.region = region != null ? region : "us-east-1";
        this.endpoint = endpoint != null ? endpoint : "";
        this.batchSize = batchSize;
        this.treatConditionalCheckFailedAsError = treatConditionalCheckFailedAsError;
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

    public boolean treatConditionalCheckFailedAsError() {
        return treatConditionalCheckFailedAsError;
    }

    public int statementColumnIndex() {
        return statementColumnIndex;
    }

}
