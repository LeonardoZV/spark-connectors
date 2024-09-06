package com.leonardozv.spark.connectors.aws.sqs;

import java.io.Serializable;

public class SQSSinkOptions implements Serializable {

    private final String region;
    private final String endpoint;
    private final String queueName;
    private final String queueOwnerAWSAccountId;
    private final int batchSize;
    private final boolean useSqsExtendedClient;
    private final String bucketName;
    private final int payloadSizeThreshold;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SQSSinkOptions(String region,
                          String endpoint,
                          String queueName,
                          String queueOwnerAWSAccountId,
                          int batchSize,
                          boolean useSqsExtendedClient,
                          String bucketName,
                          int payloadSizeThreshold,
                          int valueColumnIndex,
                          int msgAttributesColumnIndex,
                          int groupIdColumnIndex) {
        this.region = region != null ? region : "us-east-1";
        this.endpoint = endpoint != null ? endpoint : "";
        this.queueName = queueName != null ? queueName : "";
        this.queueOwnerAWSAccountId = queueOwnerAWSAccountId != null ? queueOwnerAWSAccountId : "";
        this.batchSize = batchSize;
        this.useSqsExtendedClient = useSqsExtendedClient;
        this.bucketName = bucketName;
        this.payloadSizeThreshold = payloadSizeThreshold;
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttributesColumnIndex = msgAttributesColumnIndex;
        this.groupIdColumnIndex = groupIdColumnIndex;
    }


    public String region() {
        return region;
    }

    public String endpoint() {
        return endpoint;
    }

    public String queueName() {
        return queueName;
    }

    public String queueOwnerAWSAccountId() {
        return queueOwnerAWSAccountId;
    }

    public int batchSize() {
        return batchSize;
    }

    public boolean useSqsExtendedClient() {
        return useSqsExtendedClient;
    }

    public String bucketName() {
        return bucketName;
    }

    public int payloadSizeThreshold() {
        return payloadSizeThreshold;
    }

    public int valueColumnIndex() {
        return valueColumnIndex;
    }

    public int msgAttributesColumnIndex() {
        return msgAttributesColumnIndex;
    }

    public int groupIdColumnIndex() {
        return groupIdColumnIndex;
    }

}
