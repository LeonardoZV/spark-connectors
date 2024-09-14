package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DynamoDbSinkDataWriterUnitTest {

    private DynamoDbClient dynamoDbClient;
    private DynamoDbSinkOptions options;
    private DynamoDbSinkDataWriter writer;

    @BeforeEach
    void setUp() {
        dynamoDbClient = mock(DynamoDbClient.class);

        Set<String> errorsToIgnore = new HashSet<>();
        errorsToIgnore.add(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED.toString());

        options = new DynamoDbSinkOptions.Builder()
                .region("us-west-2")
                .endpoint("http://localhost:8000")
                .batchSize(2)
                .statementColumnIndex(0)
                .errorsToIgnore(errorsToIgnore)
                .build();

        writer = new DynamoDbSinkDataWriter(0, 0, dynamoDbClient, options);
    }

    @Test
    void testWrite() {
        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("statement1");

        BatchExecuteStatementResponse response = BatchExecuteStatementResponse.builder()
                .responses(Collections.singletonList(BatchStatementResponse.builder().build()))
                .build();

        when(dynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(response);

        writer.write(row);

        ArgumentCaptor<BatchExecuteStatementRequest> requestCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);
        verify(dynamoDbClient, never()).batchExecuteStatement(requestCaptor.capture());

        writer.write(row);

        verify(dynamoDbClient, times(1)).batchExecuteStatement(requestCaptor.capture());
        BatchExecuteStatementRequest request = requestCaptor.getValue();
        assertEquals(2, request.statements().size());
        assertEquals("statement1", request.statements().get(0).statement());
    }

    @Test
    void testWriteWithErrorsAndWithoutErrorToIgnore() {

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("statement1");

        BatchStatementError error = BatchStatementError.builder().code(BatchStatementErrorCodeEnum.ACCESS_DENIED).message("Error message").build();
        BatchStatementResponse statementResponse = BatchStatementResponse.builder().error(error).build();
        BatchExecuteStatementResponse response = BatchExecuteStatementResponse.builder()
                .responses(Collections.singletonList(statementResponse))
                .build();

        ArgumentCaptor<BatchExecuteStatementRequest> requestCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);

        when(dynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(response);

        writer.write(row);

        assertThrows(DynamoDbSinkBatchResultException.class, writer::commit);

        verify(dynamoDbClient, times(1)).batchExecuteStatement(requestCaptor.capture());

        BatchExecuteStatementRequest request = requestCaptor.getValue();

        assertEquals(1, request.statements().size());
        assertEquals("statement1", request.statements().get(0).statement());

    }

    @Test
    void testWriteWithErrorsAndWithErrorToIgnore() {

        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("statement1");

        BatchStatementError error = BatchStatementError.builder().code(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED).message("Error message").build();
        BatchStatementResponse statementResponse = BatchStatementResponse.builder().error(error).build();
        BatchExecuteStatementResponse response = BatchExecuteStatementResponse.builder()
                .responses(Collections.singletonList(statementResponse))
                .build();

        ArgumentCaptor<BatchExecuteStatementRequest> requestCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);

        when(dynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(response);

        writer.write(row);

        writer.commit();

        verify(dynamoDbClient, times(1)).batchExecuteStatement(requestCaptor.capture());

        BatchExecuteStatementRequest request = requestCaptor.getValue();

        assertEquals(1, request.statements().size());
        assertEquals("statement1", request.statements().get(0).statement());

    }

    @Test
    void testCommit() {
        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("statement1");

        BatchExecuteStatementResponse response = BatchExecuteStatementResponse.builder()
                .responses(Collections.singletonList(BatchStatementResponse.builder().build()))
                .build();

        when(dynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(response);

        writer.write(row);

        WriterCommitMessage commitMessage = writer.commit();

        assertNotNull(commitMessage);
        assertInstanceOf(DynamoDbSinkWriterCommitMessage.class, commitMessage);

        ArgumentCaptor<BatchExecuteStatementRequest> requestCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);
        verify(dynamoDbClient, times(1)).batchExecuteStatement(requestCaptor.capture());
    }

    @Test
    void testCommitWithoutMoreMessages() {
        InternalRow row = mock(InternalRow.class);
        when(row.getString(0)).thenReturn("statement1");

        BatchExecuteStatementResponse response = BatchExecuteStatementResponse.builder()
                .responses(Collections.singletonList(BatchStatementResponse.builder().build()))
                .build();

        when(dynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(response);

        WriterCommitMessage commitMessage = writer.commit();

        assertNotNull(commitMessage);
        assertInstanceOf(DynamoDbSinkWriterCommitMessage.class, commitMessage);
    }

    @Test
    void testAbort() {
        assertDoesNotThrow(writer::abort);
    }

    @Test
    void testClose() {
        assertDoesNotThrow(writer::close);
    }

}
