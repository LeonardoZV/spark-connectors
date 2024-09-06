package com.leonardozv.spark.connectors.aws.sqs;

import java.io.Serializable;

public class SQSSinkOptions implements Serializable {

    private final String region;
    private final String endpoint;
    private final String queueName;
    private final String queueOwnerAWSAccountId;
    private final int batchSize;
    private final String bucketName;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SQSSinkOptions(String region,
                          String endpoint,
                          String queueName,
                          String queueOwnerAWSAccountId,
                          int batchSize,
                          String bucketName,
                          int valueColumnIndex,
                          int msgAttributesColumnIndex,
                          int groupIdColumnIndex) {
        this.region = region != null ? region : "us-east-1";
        this.endpoint = endpoint != null ? endpoint : "";
        this.queueName = queueName != null ? queueName : "";
        this.queueOwnerAWSAccountId = queueOwnerAWSAccountId != null ? queueOwnerAWSAccountId : "";
        this.batchSize = batchSize;
        this.bucketName = bucketName;
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttributesColumnIndex = msgAttributesColumnIndex;
        this.groupIdColumnIndex = groupIdColumnIndex;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getQueueName() {
        return queueName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getBucketName() {
        return bucketName;
    }

    public int getValueColumnIndex() {
        return valueColumnIndex;
    }

    public int getMsgAttributesColumnIndex() {
        return msgAttributesColumnIndex;
    }

    public int getGroupIdColumnIndex() {
        return groupIdColumnIndex;
    }

    public String getQueueOwnerAWSAccountId() {
        return queueOwnerAWSAccountId;
    }

}
