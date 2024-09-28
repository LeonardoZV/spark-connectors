package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class DynamoDbSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;

    private static final String STATEMENT_COLUMN_NAME = "statement";

    public DynamoDbSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public Write build() {
        DynamoDbSinkOptions options = new DynamoDbSinkOptions(this.info.options().asCaseSensitiveMap());
        int statementColumnIndex = this.info.schema().fieldIndex(STATEMENT_COLUMN_NAME);
        return new DynamoDbSinkWrite(options, statementColumnIndex);
    }

}