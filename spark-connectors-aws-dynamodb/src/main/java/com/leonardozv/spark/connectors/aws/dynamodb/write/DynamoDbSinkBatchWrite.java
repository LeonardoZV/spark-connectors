package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class DynamoDbSinkBatchWrite implements BatchWrite {

    private final DynamoDbSinkOptions options;
    private final StructType schema;

    public DynamoDbSinkBatchWrite(DynamoDbSinkOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new DynamoDbSinkDataWriterFactory(this.options, this.schema);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        // nothing to commit here, since this sink is not atomic
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // nothing to abort here, since this sink is not atomic
    }

}
