package com.leonardozv.spark.connectors.aws.sqs.write;

import java.io.Serializable;

public class SqsSinkOptions implements Serializable {

    private final String region;
    private final String sqsEndpoint;
    private final String queueName;
    private final String queueOwnerAWSAccountId;
    private final int batchSize;
    private final boolean useSqsExtendedClient;
    private final String s3Endpoint;
    private final boolean forcePathStyle;
    private final String bucketName;
    private final int payloadSizeThreshold;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SqsSinkOptions(Builder builder) {
        this.region = builder.region;
        this.sqsEndpoint = builder.sqsEndpoint;
        this.queueName = builder.queueName;
        this.queueOwnerAWSAccountId = builder.queueOwnerAWSAccountId;
        this.batchSize = builder.batchSize;
        this.useSqsExtendedClient = builder.useSqsExtendedClient;
        this.s3Endpoint = builder.s3Endpoint;
        this.forcePathStyle = builder.forcePathStyle;
        this.bucketName = builder.bucketName;
        this.payloadSizeThreshold = builder.payloadSizeThreshold;
        this.valueColumnIndex = builder.valueColumnIndex;
        this.msgAttributesColumnIndex = builder.msgAttributesColumnIndex;
        this.groupIdColumnIndex = builder.groupIdColumnIndex;
    }

    public String region() { return region; }
    public String sqsEndpoint() { return sqsEndpoint; }
    public String queueName() { return queueName; }
    public String queueOwnerAWSAccountId() { return queueOwnerAWSAccountId; }
    public int batchSize() { return batchSize; }
    public boolean useSqsExtendedClient() { return useSqsExtendedClient; }
    public String s3Endpoint() { return s3Endpoint; }
    public boolean forcePathStyle() { return forcePathStyle; }
    public String bucketName() { return bucketName; }
    public int payloadSizeThreshold() { return payloadSizeThreshold; }
    public int valueColumnIndex() { return valueColumnIndex; }
    public int msgAttributesColumnIndex() { return msgAttributesColumnIndex; }
    public int groupIdColumnIndex() { return groupIdColumnIndex; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements Serializable {

        private String region;
        private String sqsEndpoint;
        private String queueName;
        private String queueOwnerAWSAccountId;
        private int batchSize;
        private boolean useSqsExtendedClient;
        private String s3Endpoint;
        private boolean forcePathStyle;
        private String bucketName;
        private int payloadSizeThreshold;
        private int valueColumnIndex;
        private int msgAttributesColumnIndex;
        private int groupIdColumnIndex;

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder sqsEndpoint(String sqsEndpoint) {
            this.sqsEndpoint = sqsEndpoint;
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

        public Builder s3Endpoint(String s3Endpoint) {
            this.s3Endpoint = s3Endpoint;
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