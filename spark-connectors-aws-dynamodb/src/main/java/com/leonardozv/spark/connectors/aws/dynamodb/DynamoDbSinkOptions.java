package com.leonardozv.spark.connectors.aws.dynamodb;

import java.io.Serializable;
import java.util.Map;

public class DynamoDbSinkOptions implements Serializable {

    private final String region;
    private final String endpoint;
    private final int batchSize;
    private final Map<String, String> errorsToIgnore;
    private final int statementColumnIndex;

    public DynamoDbSinkOptions(String region,
                               String endpoint,
                               int batchSize,
                               Map<String, String> errorsToIgnore,
                               int statementColumnIndex) {
        this.region = region != null ? region : "us-east-1";
        this.endpoint = endpoint != null ? endpoint : "";
        this.batchSize = batchSize;
        this.errorsToIgnore = errorsToIgnore;
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

    public Map<String, String> errorsToIgnore() {
        return errorsToIgnore;
    }

    public int statementColumnIndex() {
        return statementColumnIndex;
    }

}
