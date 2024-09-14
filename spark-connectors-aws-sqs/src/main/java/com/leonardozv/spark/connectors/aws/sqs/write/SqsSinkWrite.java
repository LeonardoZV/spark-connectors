package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;

public class SqsSinkWrite implements Write {

    private final SqsSinkOptions options;

    public SqsSinkWrite(SqsSinkOptions options) {
        this.options = options;
    }

    @Override
    public BatchWrite toBatch() {
        return new SqsSinkBatchWrite(this.options);
    }

    public SqsSinkOptions options() { return this.options; }

}
