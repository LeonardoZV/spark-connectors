package com.leonardozv.spark.connectors.aws.sqs;

import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;

class SqsSinkDataWriterUnitTest {

    private InternalRow createInternalRow(Object... values) {
        Seq<Object> x = JavaConverters.asScalaBuffer(new ArrayList<>(Arrays.asList(values))).toSeq();
        return InternalRow.fromSeq(x);
    }

//    @Test
//    void when_ProvidedLessRowsThanBatchSize_should_NotSendMessageBatchToAWS() {
//
//        // Arrange
//        SqsClient mockSqs = mock(SqsClient.class);
//
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//
//        // Act
//        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 3, "", 0, -1, -1)) {
//            sut.write(createInternalRow(UTF8String.fromString("x")));
//        }
//
//        // Assert
//        verify(mockSqs, times(0)).sendMessageBatch(any(SendMessageBatchRequest.class));
//
//    }
//
//    @Test
//    void when_ProvidedRowsEqualsToBatchSize_should_SendMessageBatchToAWSOneTime() {
//
//        // Arrange
//        int batchSize = 3;
//        SqsClient mockSqs = mock(SqsClient.class);
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
//
//        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, batchSize, "", 0, -1, -1)) {
//            // Act
//            for(int i = 0; i < batchSize; i++) {
//                sut.write(createInternalRow(UTF8String.fromString("x")));
//            }
//        }
//
//        // Assert
//        verify(mockSqs, times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
//
//    }
//
//    @Test
//    void when_Committing_should_SendRemainingMessageBatchToAWSOneTime() {
//
//        // Arrange
//        SqsClient mockSqs = mock(SqsClient.class);
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
//
//        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 3, "", 0, -1, -1)) {
//            sut.write(createInternalRow(UTF8String.fromString("x")));
//            // Act
//            sut.commit();
//        }
//
//        // Assert
//        verify(mockSqs, times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
//
//    }
//
//    @Test
//    void when_PassingAGroupIdColumn_should_SendMessageBatchToAWSOneTimeWithGroupId() {
//
//        // Arrange
//        SqsClient mockSqs = mock(SqsClient.class);
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
//
//        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 1, "", 0, -1, 1)) {
//            // Act
//            sut.write(createInternalRow(UTF8String.fromString("x"), UTF8String.fromString("id")));
//        }
//
//        // Assert
//        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
//        verify(mockSqs).sendMessageBatch(argumentCaptor.capture());
//        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
//        assertThat(capturedArgument.entries().get(0).messageGroupId()).isEqualTo("id");
//
//    }
//
//    @Test
//    void when_PassingAMessageAttributeColumn_should_SendMessageBatchToAWSOneTimeWithMessageAttributes() {
//
//        // Arrange
//        SqsClient mockSqs = mock(SqsClient.class);
//        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().build());
//        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
//        ArrayBasedMapData map = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList("attribute-a")), new GenericArrayData(Collections.singletonList("attribute")));
//
//        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 1, "", 0, 1, 0)){
//            // Act
//            sut.write(createInternalRow(UTF8String.fromString("x"), map));
//        }
//
//        // Assert
//        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
//        verify(mockSqs).sendMessageBatch(argumentCaptor.capture());
//        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
//        assertThat(capturedArgument.entries().get(0).messageAttributes().get("attribute-a").stringValue()).isEqualTo("attribute");
//
//    }

}