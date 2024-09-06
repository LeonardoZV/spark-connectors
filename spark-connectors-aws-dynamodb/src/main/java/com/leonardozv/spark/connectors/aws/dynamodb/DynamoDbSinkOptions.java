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

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean getTreatConditionalCheckFailedAsError() {
        return treatConditionalCheckFailedAsError;
    }

    public int getStatementColumnIndex() {
        return statementColumnIndex;
    }

}
