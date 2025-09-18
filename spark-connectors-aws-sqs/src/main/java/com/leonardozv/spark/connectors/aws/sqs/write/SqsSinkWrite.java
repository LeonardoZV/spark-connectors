package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;

public class SqsSinkWrite implements Write {

    private final SqsSinkOptions options;
    private final StructType schema;

    public SqsSinkWrite(SqsSinkOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public BatchWrite toBatch() {
        return new SqsSinkBatchWrite(this.options, this.schema);
    }

    public SqsSinkOptions options() {
        return this.options;
    }

    public StructType schema() {
        return this.schema;
    }

}
