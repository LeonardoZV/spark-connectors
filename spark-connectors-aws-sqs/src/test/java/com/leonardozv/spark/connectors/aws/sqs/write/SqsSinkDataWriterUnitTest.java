package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class SqsSinkDataWriterUnitTest {

    private InternalRow createInternalRow(Object... values) {
        Seq<Object> x = JavaConverters.asScalaBuffer(new ArrayList<>(Arrays.asList(values))).toSeq();
        return InternalRow.fromSeq(x);
    }

    @Test
    void when_RowHasValueAndDelaySecondsAndMessageAttributesAndMessageGroupIdAndMessageDeduplicationIdAndBatchSizeReached_should_SendMessageBatch() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<>() {{
            put("endpoint", "http://localhost:4566");
            put("region", "us-east-1");
            put("queueOwnerAWSAccountId", "123456789012");
            put("queueName", "test-queue");
            put("batchSize", "1");
        }};

        StructType schema = new StructType()
                .add("value", "string")
                .add("delay_seconds", "integer")
                .add("message_attributes", "map<string,string>")
                .add("message_group_id", "string")
                .add("message_deduplication_id", "string");

        String queueUrl = "http://localhost:4566/123456789012/test-queue";

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());

        ArrayBasedMapData map = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList("attribute-a")), new GenericArrayData(Collections.singletonList("attribute")));
        InternalRow row = createInternalRow(UTF8String.fromString("test-message"), 1, map, UTF8String.fromString("test-group"), UTF8String.fromString("test-deduplication-id"));

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, mockSqsClient, queueUrl, new SqsSinkOptions(options), schema);

        // Act
        writer.write(row);
        writer.commit();

        // Assert
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqsClient, times(1)).sendMessageBatch(argumentCaptor.capture());
        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.queueUrl()).isEqualTo(queueUrl);
        assertThat(capturedArgument.entries()).hasSize(1);
        assertThat(capturedArgument.entries().get(0).messageBody()).isEqualTo("test-message");
        assertThat(capturedArgument.entries().get(0).delaySeconds()).isEqualTo(1);
        assertThat(capturedArgument.entries().get(0).messageAttributes().get("attribute-a").stringValue()).isEqualTo("attribute");
        assertThat(capturedArgument.entries().get(0).messageGroupId()).isEqualTo("test-group");
        assertThat(capturedArgument.entries().get(0).messageDeduplicationId()).isEqualTo("test-deduplication-id");

    }

    @Test
    void when_RowHasValueOnlyAndAndBatchSizeReachedAndSqsRespondsWithError_should_SendMessageBatchAndThrowException() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<>() {{
            put("endpoint", "http://localhost:4566");
            put("region", "us-east-1");
            put("queueOwnerAWSAccountId", "123456789012");
            put("queueName", "test-queue");
            put("batchSize", "1");
        }};

        StructType schema = new StructType()
                .add("value", "string");

        String queueUrl = "http://localhost:4566/123456789012/test-queue";

        SqsClient mockSqsClient = mock(SqsClient.class);
        SendMessageBatchResponse mockResponse = mock(SendMessageBatchResponse.class);
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(mockResponse);

        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder().id("1").message("Error message").build();
        when(mockResponse.failed()).thenReturn(Collections.singletonList(errorEntry));

        InternalRow row = createInternalRow(UTF8String.fromString("test-message"));

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, mockSqsClient, queueUrl, new SqsSinkOptions(options), schema);

        // Act & Assert
        assertThrows(SqsSinkBatchResultException.class, () -> writer.write(row));
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqsClient, times(1)).sendMessageBatch(argumentCaptor.capture());
        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.queueUrl()).isEqualTo(queueUrl);
        assertThat(capturedArgument.entries()).hasSize(1);
        assertThat(capturedArgument.entries().get(0).messageBody()).isEqualTo("test-message");

    }

    @Test
    void when_RowHasValueAndMessageAttributesAndMessageGroupIdAndBatchSizeNotReachedButCommitCalled_should_SendMessageBatch() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<>() {{
            put("endpoint", "http://localhost:4566");
            put("region", "us-east-1");
            put("queueOwnerAWSAccountId", "123456789012");
            put("queueName", "test-queue");
            put("batchSize", "2");
        }};

        StructType schema = new StructType()
                .add("value", "string")
                .add("message_attributes", "map<string,string>")
                .add("message_group_id", "string");

        String queueUrl = "http://localhost:4566/123456789012/test-queue";

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());

        ArrayBasedMapData map = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList("attribute-a")), new GenericArrayData(Collections.singletonList("attribute")));
        InternalRow row = createInternalRow(UTF8String.fromString("test-message"), map, UTF8String.fromString("test-group"));

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, mockSqsClient, queueUrl, new SqsSinkOptions(options), schema);

        // Act
        writer.write(row);
        writer.commit();

        // Assert
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqsClient, times(1)).sendMessageBatch(argumentCaptor.capture());
        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.queueUrl()).isEqualTo(queueUrl);
        assertThat(capturedArgument.entries()).hasSize(1);
        assertThat(capturedArgument.entries().get(0).messageBody()).isEqualTo("test-message");
        assertThat(capturedArgument.entries().get(0).messageAttributes().get("attribute-a").stringValue()).isEqualTo("attribute");
        assertThat(capturedArgument.entries().get(0).messageGroupId()).isEqualTo("test-group");

    }

    @Test
    void when_AbortCalled_should_DoNothing() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<>() {{
            put("endpoint", "http://localhost:4566");
            put("region", "us-east-1");
            put("queueOwnerAWSAccountId", "123456789012");
            put("queueName", "test-queue");
            put("batchSize", "10");
        }};

        StructType schema = new StructType()
                .add("value", "string")
                .add("message_attributes", "map<string,string>")
                .add("message_group_id", "string");

        SqsClient mockSqsClient = mock(SqsClient.class);

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, mockSqsClient, "http://localhost:4566/123456789012/test-queue", new SqsSinkOptions(options), schema);

        // Act & Assert
        assertDoesNotThrow(writer::abort);

    }

}