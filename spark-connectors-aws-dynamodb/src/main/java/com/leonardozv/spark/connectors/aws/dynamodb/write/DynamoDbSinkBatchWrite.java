package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class DynamoDbSinkBatchWrite implements BatchWrite {

    private final DynamoDbSinkOptions options;
    private final int statementColumnIndex;

    public DynamoDbSinkBatchWrite(DynamoDbSinkOptions options, int statementColumnIndex) {
        this.options = options;
        this.statementColumnIndex = statementColumnIndex;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new DynamoDbSinkDataWriterFactory(this.options, this.statementColumnIndex);
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
