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
        SqsSinkOptions options = new SqsSinkOptions(this.info.options().asCaseSensitiveMap());
        int valueColumnIndex = this.info.schema().fieldIndex(VALUE_COLUMN_NAME);
        int msgAttributesColumnIndex = this.info.schema().getFieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME).isEmpty() ? -1 : info.schema().fieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME);
        int groupIdColumnIndex = this.info.schema().getFieldIndex(GROUP_ID_COLUMN_NAME).isEmpty() ? -1 : info.schema().fieldIndex(GROUP_ID_COLUMN_NAME);
        return new SqsSinkWrite(options, valueColumnIndex, msgAttributesColumnIndex, groupIdColumnIndex);

    }

}
