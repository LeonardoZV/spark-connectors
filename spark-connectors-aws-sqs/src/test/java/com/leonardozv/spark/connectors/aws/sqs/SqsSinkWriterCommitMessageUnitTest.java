package com.leonardozv.spark.connectors.aws.sqs;

import com.leonardozv.spark.connectors.aws.sqs.write.SqsSinkWriterCommitMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SqsSinkWriterCommitMessageUnitTest {

    @Test
    void testEquals() {
        SqsSinkWriterCommitMessage message1 = new SqsSinkWriterCommitMessage(1, 100L);
        SqsSinkWriterCommitMessage message2 = new SqsSinkWriterCommitMessage(1, 100L);
        SqsSinkWriterCommitMessage message3 = new SqsSinkWriterCommitMessage(2, 200L);

        assertEquals(message1, message2, "Messages with the same partitionId and taskId should be equal");
        assertNotEquals(message1, message3, "Messages with different partitionId should not be equal");
    }

    @Test
    void testHashCode() {
        SqsSinkWriterCommitMessage message1 = new SqsSinkWriterCommitMessage(1, 100L);
        SqsSinkWriterCommitMessage message2 = new SqsSinkWriterCommitMessage(1, 100L);
        SqsSinkWriterCommitMessage message3 = new SqsSinkWriterCommitMessage(2, 200L);

        assertEquals(message1.hashCode(), message2.hashCode(), "Hash codes should be equal for messages with the same partitionId and taskId");
        assertNotEquals(message1.hashCode(), message3.hashCode(), "Hash codes should be different for messages with different partitionId");
    }

}
