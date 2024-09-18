package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqsSinkDataWriterUnitTest {

    private InternalRow createInternalRow(Object... values) {
        Seq<Object> x = JavaConverters.asScalaBuffer(new ArrayList<>(Arrays.asList(values))).toSeq();
        return InternalRow.fromSeq(x);
    }

    @Test
    void testWrite() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(2)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("test-message");
        when(row.getMap(1)).thenReturn(mock(MapData.class));
        when(row.getString(2)).thenReturn("test-group");

        writer.write(row);

        ArgumentCaptor<SendMessageBatchRequest> captor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqsClient, never()).sendMessageBatch(captor.capture());

    }

    @Test
    void testWriteWithoutMessageAttributesAndGroupId() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(-1)
                .groupIdColumnIndex(-1)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("test-message");
        when(row.getMap(1)).thenReturn(mock(MapData.class));
        when(row.getString(2)).thenReturn("test-group");

        writer.write(row);

        ArgumentCaptor<SendMessageBatchRequest> captor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqsClient, never()).sendMessageBatch(captor.capture());

    }

    @Test
    void testWriteWithMessageAttributes() {

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(1)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(0)
                .build();

        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
        ArrayBasedMapData map = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList("attribute-a")), new GenericArrayData(Collections.singletonList("attribute")));
        try(SqsSinkDataWriter sut = new SqsSinkDataWriter(1, 2, mockSqs, "http://localhost:4566/123456789012/test-queue", options)){
            // Act
            sut.write(createInternalRow(UTF8String.fromString("x"), map));
        }
        // Assert
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqs).sendMessageBatch(argumentCaptor.capture());
        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.entries().get(0).messageAttributes().get("attribute-a").stringValue()).isEqualTo("attribute");

    }

    @Test
    void testWriteWithBatchSizeReached() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(2)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("test-message");
        when(row.getMap(1)).thenReturn(mock(MapData.class));
        when(row.getString(2)).thenReturn("test-group");

        SendMessageBatchResponse mockResponse = mock(SendMessageBatchResponse.class);
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(mockResponse);

        for (int i = 0; i < 10; i++) {
            writer.write(row);
        }

        ArgumentCaptor<SendMessageBatchRequest> captor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqsClient, times(1)).sendMessageBatch(captor.capture());

        SendMessageBatchRequest request = captor.getValue();
        assertEquals(10, request.entries().size());
    }

    @Test
    void testCommit() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(2)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("test-message");
        when(row.getMap(1)).thenReturn(mock(MapData.class));
        when(row.getString(2)).thenReturn("test-group");

        SendMessageBatchResponse mockResponse = mock(SendMessageBatchResponse.class);
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(mockResponse);

        writer.write(row);
        writer.commit();

        ArgumentCaptor<SendMessageBatchRequest> captor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(sqsClient, times(1)).sendMessageBatch(captor.capture());

        SendMessageBatchRequest request = captor.getValue();
        assertEquals(1, request.entries().size());

    }

    @Test
    void testCommitWithErrors() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(2)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("test-message");
        when(row.getMap(1)).thenReturn(mock(MapData.class));
        when(row.getString(2)).thenReturn("test-group");

        BatchResultErrorEntry errorEntry = BatchResultErrorEntry.builder()
                .id("1")
                .message("Error message")
                .build();

        SendMessageBatchResponse mockResponse = mock(SendMessageBatchResponse.class);
        when(mockResponse.failed()).thenReturn(Collections.singletonList(errorEntry));
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(mockResponse);

        writer.write(row);

        assertThrows(SqsSinkBatchResultException.class, writer::commit);

    }

    @Test
    void testCommitWithoutMoreMessages() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(2)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("test-message");
        when(row.getMap(1)).thenReturn(mock(MapData.class));
        when(row.getString(2)).thenReturn("test-group");

        SendMessageBatchResponse mockResponse = mock(SendMessageBatchResponse.class);
        when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(mockResponse);

        WriterCommitMessage commitMessage = writer.commit();

        assertNotNull(commitMessage);
        assertInstanceOf(SqsSinkWriterCommitMessage.class, commitMessage);

    }

    @Test
    void testAbort() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(2)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        assertDoesNotThrow(writer::abort);

    }

    @Test
    void testClose() {

        SqsClient sqsClient = mock(SqsClient.class);

        SqsSinkOptions options = SqsSinkOptions.builder()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .queueName("test-queue")
                .queueOwnerAWSAccountId("123456789012")
                .batchSize(10)
                .valueColumnIndex(0)
                .msgAttributesColumnIndex(1)
                .groupIdColumnIndex(2)
                .build();

        SqsSinkDataWriter writer = new SqsSinkDataWriter(0, 0, sqsClient, "http://localhost:4566/123456789012/test-queue", options);

        assertDoesNotThrow(writer::close);

    }

}