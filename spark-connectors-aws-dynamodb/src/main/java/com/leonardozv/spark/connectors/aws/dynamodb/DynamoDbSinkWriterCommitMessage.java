package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Objects;

public class DynamoDbSinkWriterCommitMessage implements WriterCommitMessage {

    private final int partitionId;
    private final long taskId;

    public DynamoDbSinkWriterCommitMessage(int partitionId, long taskId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DynamoDbSinkWriterCommitMessage)) return false;
        DynamoDbSinkWriterCommitMessage that = (DynamoDbSinkWriterCommitMessage) o;
        return partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId + taskId);
    }
}
