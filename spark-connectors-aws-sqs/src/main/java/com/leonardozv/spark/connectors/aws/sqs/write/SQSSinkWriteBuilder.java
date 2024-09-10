package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class SQSSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;

    private static final String MESSAGE_ATTRIBUTES_COLUMN_NAME = "msg_attributes";
    private static final String GROUP_ID_COLUMN_NAME = "group_id";
    private static final String VALUE_COLUMN_NAME = "value";

    public SQSSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public Write build() {

        SQSSinkOptions options = SQSSinkOptions.builder()
                .region(info.options().getOrDefault("region", "us-east-1"))
                .endpoint(info.options().getOrDefault("endpoint", ""))
                .queueName(info.options().getOrDefault("queueName", ""))
                .queueOwnerAWSAccountId(info.options().getOrDefault("queueOwnerAWSAccountId", ""))
                .batchSize(Integer.parseInt(info.options().getOrDefault("batchSize", "10")))
                .useSqsExtendedClient(Boolean.parseBoolean(info.options().getOrDefault("useSqsExtendedClient", "false")))
                .bucketName(info.options().getOrDefault("bucketName", ""))
                .payloadSizeThreshold(Integer.parseInt(info.options().getOrDefault("payloadSizeThreshold", "-1")))
                .valueColumnIndex(info.schema().fieldIndex(VALUE_COLUMN_NAME))
                .msgAttributesColumnIndex(info.schema().getFieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME).isEmpty() ? -1 : info.schema().fieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME))
                .groupIdColumnIndex(info.schema().getFieldIndex(GROUP_ID_COLUMN_NAME).isEmpty() ? -1 : info.schema().fieldIndex(GROUP_ID_COLUMN_NAME))
                .build();

        return new SQSSinkWrite(options);

    }

}
