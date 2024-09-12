package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class SqsSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;

    private static final String MESSAGE_ATTRIBUTES_COLUMN_NAME = "msg_attributes";
    private static final String GROUP_ID_COLUMN_NAME = "group_id";
    private static final String VALUE_COLUMN_NAME = "value";

    public SqsSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public Write build() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .region(this.info.options().getOrDefault("region", "us-east-1"))
                .endpoint(this.info.options().getOrDefault("endpoint", ""))
                .queueName(this.info.options().getOrDefault("queueName", ""))
                .queueOwnerAWSAccountId(this.info.options().getOrDefault("queueOwnerAWSAccountId", ""))
                .batchSize(Integer.parseInt(this.info.options().getOrDefault("batchSize", "10")))
                .useSqsExtendedClient(Boolean.parseBoolean(this.info.options().getOrDefault("useSqsExtendedClient", "false")))
                .bucketName(this.info.options().getOrDefault("bucketName", ""))
                .payloadSizeThreshold(Integer.parseInt(this.info.options().getOrDefault("payloadSizeThreshold", "-1")))
                .valueColumnIndex(this.info.schema().fieldIndex(VALUE_COLUMN_NAME))
                .msgAttributesColumnIndex(this.info.schema().getFieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME).isEmpty() ? -1 : info.schema().fieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME))
                .groupIdColumnIndex(this.info.schema().getFieldIndex(GROUP_ID_COLUMN_NAME).isEmpty() ? -1 : info.schema().fieldIndex(GROUP_ID_COLUMN_NAME))
                .build();

        return new SqsSinkWrite(options);

    }

}
