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
import software.amazon.awssdk.services.sqs.model.*;

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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
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
                .add("message_deduplication_id", "string")
                .add("message_group_id", "string")
                .add("message_system_attributes", "map<string,string>");

        String queueUrl = "http://localhost:4566/123456789012/test-queue";

        SqsClient mockSqsClient = mock(SqsClient.class);
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());

        ArrayBasedMapData mapMessageAttributes = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList("attribute-a")), new GenericArrayData(Collections.singletonList("attribute")));
        ArrayBasedMapData mapMessageSystemAttributes = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER.toString())), new GenericArrayData(Collections.singletonList("attribute")));
        InternalRow row = createInternalRow(UTF8String.fromString("test-message"), 1, mapMessageAttributes, UTF8String.fromString("test-deduplication-id"), UTF8String.fromString("test-group"), mapMessageSystemAttributes);

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
        assertThat(capturedArgument.entries().get(0).messageDeduplicationId()).isEqualTo("test-deduplication-id");
        assertThat(capturedArgument.entries().get(0).messageGroupId()).isEqualTo("test-group");
        assertThat(capturedArgument.entries().get(0).messageSystemAttributes().get(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER).stringValue()).isEqualTo("attribute");

    }

    @Test
    void when_RowHasValueOnlyAndAndBatchSizeReachedAndSqsRespondsWithError_should_SendMessageBatchAndThrowException() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
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
        assertThrows(ResponseContainsNonRetryableErrorsException.class, () -> writer.write(row));
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
        Map<String, String> options = new LinkedHashMap<String, String>() {{
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

        ArrayBasedMapData mapMessageAttributes = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList("attribute-a")), new GenericArrayData(Collections.singletonList("attribute")));
        InternalRow row = createInternalRow(UTF8String.fromString("test-message"), mapMessageAttributes, UTF8String.fromString("test-group"));

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
    void when_RowHasValueAndBatchSizeReachedAndHasRetryErrorsAndSqsRespondsWithError_should_SendMessageBatchAndThrowExceptionWhenMaxAttemptsReached() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:4566");
            put("region", "us-east-1");
            put("queueOwnerAWSAccountId", "123456789012");
            put("queueName", "test-queue");
            put("batchSize", "2");
            put("retryErrors", "ThrottlingError");
        }};

        StructType schema = new StructType()
                .add("message_id", "string")
                .add("value", "string");


        String queueUrl = "http://localhost:4566/123456789012/test-queue";

        SqsClient mockSqsClient = mock(SqsClient.class);
        SendMessageBatchResultEntry resultEntry = SendMessageBatchResultEntry.builder().build();
        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder().id("id-test-message-2").code("ThrottlingError").message("Error message").build();
        SendMessageBatchResponse firstResponse = SendMessageBatchResponse.builder().successful(resultEntry).failed(errorEntry).build();
        SendMessageBatchResponse secondResponse = SendMessageBatchResponse.builder().failed(errorEntry).build();
        SendMessageBatchResponse thirdResponse = SendMessageBatchResponse.builder().failed(errorEntry).build();
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(firstResponse, secondResponse, thirdResponse);

        InternalRow row1 = createInternalRow(UTF8String.fromString("id-test-message-1"), UTF8String.fromString("test-message-1"));
        InternalRow row2 = createInternalRow(UTF8String.fromString("id-test-message-2"), UTF8String.fromString("test-message-2"));

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, mockSqsClient, queueUrl, new SqsSinkOptions(options), schema);

        // Act & Assert
        assertDoesNotThrow(() -> writer.write(row1));
        assertThrows(ResponseContainsRetryableErrorsException.class, () -> writer.write(row2));
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqsClient, times(3)).sendMessageBatch(argumentCaptor.capture());
        List<SendMessageBatchRequest> capturedArgument = argumentCaptor.getAllValues();
        assertThat(capturedArgument.get(0).entries()).hasSize(2);
        assertThat(capturedArgument.get(0).entries().get(0).messageBody()).isEqualTo("test-message-1");
        assertThat(capturedArgument.get(0).entries().get(1).messageBody()).isEqualTo("test-message-2");
        assertThat(capturedArgument.get(1).entries()).hasSize(1);
        assertThat(capturedArgument.get(1).entries().get(0).messageBody()).isEqualTo("test-message-2");
        assertThat(capturedArgument.get(2).entries()).hasSize(1);
        assertThat(capturedArgument.get(2).entries().get(0).messageBody()).isEqualTo("test-message-2");

    }

    @Test
    void when_RowHasValueAndBatchSizeReachedAndHasIgnoreErrorsAndSqsRespondsWithError_should_SendMessageBatchAndNotThrowException() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:4566");
            put("region", "us-east-1");
            put("queueOwnerAWSAccountId", "123456789012");
            put("queueName", "test-queue");
            put("batchSize", "1");
            put("ignoreErrors", "ThrottlingError");
        }};

        StructType schema = new StructType()
                .add("message_id", "string")
                .add("value", "string");


        String queueUrl = "http://localhost:4566/123456789012/test-queue";

        SqsClient mockSqsClient = mock(SqsClient.class);
        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder().id("id-test-message-1").code("ThrottlingError").message("Error message").build();
        SendMessageBatchResponse response = SendMessageBatchResponse.builder().failed(errorEntry).build();
        when(mockSqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(response);

        InternalRow row1 = createInternalRow(UTF8String.fromString("id-test-message-1"), UTF8String.fromString("test-message-1"));

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, mockSqsClient, queueUrl, new SqsSinkOptions(options), schema);

        // Act & Assert
        assertDoesNotThrow(() -> writer.write(row1));
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqsClient, times(1)).sendMessageBatch(argumentCaptor.capture());
        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.entries()).hasSize(1);
        assertThat(capturedArgument.entries().get(0).messageBody()).isEqualTo("test-message-1");

    }

    @Test
    void when_AbortCalled_should_DoNothing() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
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