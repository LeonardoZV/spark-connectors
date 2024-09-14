package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class DynamoDbSinkWriterCommitMessageUnitTest {

    @Test
    void testEquals() {
        DynamoDbSinkWriterCommitMessage message1 = new DynamoDbSinkWriterCommitMessage(1, 100L);
        DynamoDbSinkWriterCommitMessage message2 = new DynamoDbSinkWriterCommitMessage(1, 100L);
        DynamoDbSinkWriterCommitMessage message3 = new DynamoDbSinkWriterCommitMessage(2, 200L);

        assertEquals(message1, message2, "Messages with the same partitionId and taskId should be equal");
        assertNotEquals(message1, message3, "Messages with different partitionId should not be equal");
    }

    @Test
    void testHashCode() {
        DynamoDbSinkWriterCommitMessage message1 = new DynamoDbSinkWriterCommitMessage(1, 100L);
        DynamoDbSinkWriterCommitMessage message2 = new DynamoDbSinkWriterCommitMessage(1, 100L);
        DynamoDbSinkWriterCommitMessage message3 = new DynamoDbSinkWriterCommitMessage(2, 200L);

        assertEquals(message1.hashCode(), message2.hashCode(), "Hash codes should be equal for messages with the same partitionId and taskId");
        assertNotEquals(message1.hashCode(), message3.hashCode(), "Hash codes should be different for messages with different partitionId");
    }

}
