package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;

public class DynamoDbSinkWrite implements Write {

    private final DynamoDbSinkOptions options;
    private final StructType schema;

    public DynamoDbSinkWrite(DynamoDbSinkOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public BatchWrite toBatch() {
        return new DynamoDbSinkBatchWrite(this.options, this.schema);
    }

    public DynamoDbSinkOptions options() {
        return this.options;
    }
    public StructType schema() {
        return this.schema;
    }

}
