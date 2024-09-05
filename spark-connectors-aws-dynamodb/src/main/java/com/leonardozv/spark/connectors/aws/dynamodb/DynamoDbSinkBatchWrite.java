package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class DynamoDbSinkBatchWrite implements BatchWrite {

    private DynamoDbSinkOptions options;

    public DynamoDbSinkBatchWrite(DynamoDbSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new DynamoDbSinkDataWriterFactory(options);
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
