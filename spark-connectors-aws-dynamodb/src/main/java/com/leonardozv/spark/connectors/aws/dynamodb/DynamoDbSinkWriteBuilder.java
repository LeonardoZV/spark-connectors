package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class DynamoDbSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;
    private static final String STATEMENT_COLUMN_NAME = "statement";

    public DynamoDbSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public Write build() {
        int batchSize = Integer.parseInt(info.options().getOrDefault("batchSize", "25"));
        boolean treatConditionalCheckFailedAsError = Boolean.parseBoolean(info.options().getOrDefault("treatConditionalCheckFailedAsError", "true"));
        final StructType schema = info.schema();
        DynamoDbSinkOptions options = new DynamoDbSinkOptions(
                info.options().get("region"),
                info.options().get("endpoint"),
                batchSize,
                treatConditionalCheckFailedAsError,
                schema.fieldIndex(STATEMENT_COLUMN_NAME));
        return new DynamoDbSinkWrite(options);
    }

}