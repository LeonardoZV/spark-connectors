package com.leonardozv.spark.connectors.aws.sqs;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

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
        int batchSize = Integer.parseInt(info.options().getOrDefault("batchSize", "10"));
        boolean useSqsExtendedClient = Boolean.parseBoolean(info.options().getOrDefault("useSqsExtendedClient", "false"));
        int payloadSizeThreshold = Integer.parseInt(info.options().getOrDefault("payloadSizeThreshold", "-1"));
        final StructType schema = info.schema();
        SQSSinkOptions options = new SQSSinkOptions(
                info.options().get("region"),
                info.options().get("endpoint"),
                info.options().get("queueName"),
                info.options().get("queueOwnerAWSAccountId"),
                batchSize,
                useSqsExtendedClient,
                info.options().get("bucketName"),
                payloadSizeThreshold,
                schema.fieldIndex(VALUE_COLUMN_NAME),
                schema.getFieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME).isEmpty() ? -1 : schema.fieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME),
                schema.getFieldIndex(GROUP_ID_COLUMN_NAME).isEmpty() ? -1 : schema.fieldIndex(GROUP_ID_COLUMN_NAME));
        return new SQSSinkWrite(options);
    }

}
