package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;

public class SqsSinkWrite implements Write {

    private final SqsSinkOptions options;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SqsSinkWrite(SqsSinkOptions options, int valueColumnIndex, int msgAttributesColumnIndex, int groupIdColumnIndex) {
        this.options = options;
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttributesColumnIndex = msgAttributesColumnIndex;
        this.groupIdColumnIndex = groupIdColumnIndex;
    }

    @Override
    public BatchWrite toBatch() {
        return new SqsSinkBatchWrite(this.options, valueColumnIndex, msgAttributesColumnIndex, groupIdColumnIndex);
    }

    public SqsSinkOptions options() { return this.options; }
    public int valueColumnIndex() { return this.valueColumnIndex; }
    public int msgAttributesColumnIndex() { return this.msgAttributesColumnIndex; }
    public int groupIdColumnIndex() { return this.groupIdColumnIndex; }

}
