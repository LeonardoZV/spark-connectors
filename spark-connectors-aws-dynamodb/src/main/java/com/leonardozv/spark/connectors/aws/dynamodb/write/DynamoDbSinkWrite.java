package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;

public class DynamoDbSinkWrite implements Write {

    private final DynamoDbSinkOptions options;

    public DynamoDbSinkWrite(DynamoDbSinkOptions options) {
        this.options = options;
    }

    @Override
    public BatchWrite toBatch() {
        return new DynamoDbSinkBatchWrite(options);
    }

}
