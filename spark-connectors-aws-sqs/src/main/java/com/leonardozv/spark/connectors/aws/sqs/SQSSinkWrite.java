package com.leonardozv.spark.connectors.aws.sqs;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;

public class SQSSinkWrite implements Write {

    private final SQSSinkOptions options;

    public SQSSinkWrite(SQSSinkOptions options) {
        this.options = options;
    }

    @Override
    public BatchWrite toBatch() {
        return new SQSSinkBatchWrite(options);
    }

}
