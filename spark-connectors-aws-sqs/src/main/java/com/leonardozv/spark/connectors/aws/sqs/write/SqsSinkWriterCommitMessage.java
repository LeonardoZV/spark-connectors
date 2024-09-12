package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Objects;

public class SqsSinkWriterCommitMessage implements WriterCommitMessage {

    private final int partitionId;
    private final long taskId;

    public SqsSinkWriterCommitMessage(int partitionId, long taskId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SqsSinkWriterCommitMessage)) return false;
        SqsSinkWriterCommitMessage that = (SqsSinkWriterCommitMessage) o;
        return this.partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.partitionId + this.taskId);
    }

}
