package com.leonardozv.spark.connectors.aws.sqs;

import com.leonardozv.spark.connectors.aws.sqs.write.SQSSinkWriteBuilder;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.HashSet;
import java.util.Set;

public class SQSTable implements SupportsWrite {

    private final StructType schema;
    private Set<TableCapability> capabilities;

    public SQSTable(StructType schema) {
        this.schema = schema;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new SQSSinkWriteBuilder(info);
    }

    @Override
    public String name() {
        return "AWS-SQS";
    }

    @Override
    public StructType schema() {
        return this.schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            this.capabilities.add(TableCapability.BATCH_WRITE);
        }
        return capabilities;
    }

}
