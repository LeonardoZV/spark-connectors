package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import software.amazon.awssdk.services.dynamodb.model.BatchStatementErrorCodeEnum;

import java.util.Arrays;
import java.util.stream.Collectors;

public class DynamoDbSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;
    private static final String STATEMENT_COLUMN_NAME = "statement";

    public DynamoDbSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public Write build() {

        DynamoDbSinkOptions options = DynamoDbSinkOptions.builder()
                .region(this.info.options().getOrDefault("region", "us-east-1"))
                .endpoint(this.info.options().get("endpoint"))
                .batchSize(Integer.parseInt(this.info.options().getOrDefault("batchSize", "25")))
                .errorsToIgnore(Arrays.stream(this.info.options().getOrDefault("errorsToIgnore", "").split(",")).filter(s -> !s.isEmpty()).collect(Collectors.toSet()))
                .statementColumnIndex(this.info.schema().fieldIndex(STATEMENT_COLUMN_NAME))
                .build();

        BatchStatementErrorCodeEnum.fromValue("ConditionalCheckFailed");
        return new DynamoDbSinkWrite(options);

    }

}