package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class DynamoDbSinkWriteBuilder implements WriteBuilder {

    private final DynamoDbSinkOptions options;
    private final StructType schema;

    public DynamoDbSinkWriteBuilder(DynamoDbSinkOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public Write build() {
        return new DynamoDbSinkWrite(this.options, this.schema);
    }

}