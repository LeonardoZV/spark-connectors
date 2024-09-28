package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;

public class DynamoDbSinkWrite implements Write {

    private final DynamoDbSinkOptions options;
    private final int statementColumnIndex;

    public DynamoDbSinkWrite(DynamoDbSinkOptions options, int statementColumnIndex) {
        this.options = options;
        this.statementColumnIndex = statementColumnIndex;
    }

    @Override
    public BatchWrite toBatch() {
        return new DynamoDbSinkBatchWrite(this.options, this.statementColumnIndex);
    }

    public DynamoDbSinkOptions options() {
        return options;
    }

    public int statementColumnIndex() {
        return statementColumnIndex;
    }

}
