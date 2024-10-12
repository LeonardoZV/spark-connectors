package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class SqsSinkBatchWrite implements BatchWrite {

    private final SqsSinkOptions options;
    private final StructType schema;

    public SqsSinkBatchWrite(SqsSinkOptions options, StructType schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new SqsSinkDataWriterFactory(this.options, this.schema);
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
