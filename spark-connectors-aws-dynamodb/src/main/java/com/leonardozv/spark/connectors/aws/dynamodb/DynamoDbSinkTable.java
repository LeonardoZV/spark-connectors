package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.HashSet;
import java.util.Set;

public class DynamoDbSinkTable implements SupportsWrite {

    private Set<TableCapability> capabilities;
    private final StructType schema;

    public DynamoDbSinkTable(StructType schema) {
        this.schema = schema;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new DynamoDbSinkWriteBuilder(info);
    }

    @Override
    public String name() {
        return "AWS-DYNAMODB";
    }

    @Override
    public StructType schema() {
        return this.schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_WRITE);
        }
        return capabilities;
    }
}
