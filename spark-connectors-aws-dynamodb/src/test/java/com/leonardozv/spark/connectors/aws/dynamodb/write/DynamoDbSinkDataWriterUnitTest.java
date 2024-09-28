package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class DynamoDbSinkDataWriterUnitTest {

    private InternalRow createInternalRow(Object... values) {
        Seq<Object> x = JavaConverters.asScalaBuffer(new ArrayList<>(Arrays.asList(values))).toSeq();
        return InternalRow.fromSeq(x);
    }

    @Test
    void when_RowHasStatementAndBatchSizeReached_should_ExecuteBatchExecuteStatement() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:8000");
            put("region", "us-west-2");
            put("batchSize", "1");
        }};

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);
        BatchExecuteStatementResponse response = BatchExecuteStatementResponse.builder().responses(Collections.singletonList(BatchStatementResponse.builder().build())).build();
        when(mockDynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(response);

        InternalRow row = createInternalRow(UTF8String.fromString("test-statement"));

        DynamoDbSinkDataWriter writer = new DynamoDbSinkDataWriter(0, 0, mockDynamoDbClient, new DynamoDbSinkOptions(options), 0);

        // Act
        writer.write(row);
        writer.commit();

        // Assert
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<BatchExecuteStatementRequest> argumentCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);
        verify(mockDynamoDbClient, times(1)).batchExecuteStatement(argumentCaptor.capture());
        BatchExecuteStatementRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.statements()).hasSize(1);
        assertThat(capturedArgument.statements().get(0).statement()).isEqualTo("test-statement");

    }

    @Test
    void when_RowHasStatementAndBatchSizeReachedAndDynamoDbRespondsWithError_should_ExecuteBatchExecuteStatementAndThrowException() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:8000");
            put("region", "us-west-2");
            put("batchSize", "1");
        }};

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);
        BatchStatementError error = BatchStatementError.builder().code(BatchStatementErrorCodeEnum.ACCESS_DENIED).message("Error message").build();
        BatchStatementResponse statementResponse = BatchStatementResponse.builder().error(error).build();
        BatchExecuteStatementResponse batchStatementResponse = BatchExecuteStatementResponse.builder().responses(Collections.singletonList(statementResponse)).build();
        when(mockDynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(batchStatementResponse);

        InternalRow row = createInternalRow(UTF8String.fromString("test-statement"));

        DynamoDbSinkDataWriter writer = new DynamoDbSinkDataWriter(0, 0, mockDynamoDbClient, new DynamoDbSinkOptions(options), 0);

        // Act & Assert
        assertThrows(DynamoDbSinkBatchResultException.class, () -> writer.write(row));
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<BatchExecuteStatementRequest> argumentCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);
        verify(mockDynamoDbClient, times(1)).batchExecuteStatement(argumentCaptor.capture());
        BatchExecuteStatementRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.statements()).hasSize(1);
        assertThat(capturedArgument.statements().get(0).statement()).isEqualTo("test-statement");

    }

    @Test
    void when_RowHasStatementAndBatchSizeReachedAndHasErrorsToIgnoreAndDynamoDbRespondsWithError_should_ExecuteBatchExecuteStatementAndNotThrowException() {

        Set<String> errorsToIgnore = new HashSet<>();
        errorsToIgnore.add(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED.toString());

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:8000");
            put("region", "us-west-2");
            put("batchSize", "1");
            put("errorsToIgnore", String.join(",", errorsToIgnore));
        }};

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);
        BatchStatementError error = BatchStatementError.builder().code(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED).message("Error message").build();
        BatchStatementResponse statementResponse = BatchStatementResponse.builder().error(error).build();
        BatchExecuteStatementResponse batchStatementResponse = BatchExecuteStatementResponse.builder().responses(Collections.singletonList(statementResponse)).build();
        when(mockDynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(batchStatementResponse);

        InternalRow row = createInternalRow(UTF8String.fromString("test-statement"));

        DynamoDbSinkDataWriter writer = new DynamoDbSinkDataWriter(0, 0, mockDynamoDbClient, new DynamoDbSinkOptions(options), 0);

        // Act & Assert
        assertDoesNotThrow(() -> writer.write(row));
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<BatchExecuteStatementRequest> argumentCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);
        verify(mockDynamoDbClient, times(1)).batchExecuteStatement(argumentCaptor.capture());
        BatchExecuteStatementRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.statements()).hasSize(1);
        assertThat(capturedArgument.statements().get(0).statement()).isEqualTo("test-statement");

    }

    @Test
    void when_RowHasStatementAndBatchSizeNotReachedButCommitCalled_should_ExecuteBatchExecuteStatement() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:8000");
            put("region", "us-west-2");
            put("batchSize", "2");
        }};

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);
        when(mockDynamoDbClient.batchExecuteStatement(any(BatchExecuteStatementRequest.class))).thenReturn(BatchExecuteStatementResponse.builder().build());

        InternalRow row = createInternalRow(UTF8String.fromString("test-statement"));

        DynamoDbSinkDataWriter writer = new DynamoDbSinkDataWriter(0, 0, mockDynamoDbClient, new DynamoDbSinkOptions(options), 0);

        // Act
        writer.write(row);
        writer.commit();

        // Assert
        assertDoesNotThrow(writer::close);
        ArgumentCaptor<BatchExecuteStatementRequest> argumentCaptor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);
        verify(mockDynamoDbClient, times(1)).batchExecuteStatement(argumentCaptor.capture());
        BatchExecuteStatementRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.statements()).hasSize(1);
        assertThat(capturedArgument.statements().get(0).statement()).isEqualTo("test-statement");

    }

    @Test
    void when_AbortCalled_should_DoNothing() {

        // Arrange
        Map<String, String> options = new LinkedHashMap<String, String>() {{
            put("endpoint", "http://localhost:8000");
            put("region", "us-west-2");
            put("batchSize", "2");
        }};

        DynamoDbClient mockDynamoDbClient = mock(DynamoDbClient.class);

        DynamoDbSinkDataWriter writer = new DynamoDbSinkDataWriter(0, 0, mockDynamoDbClient, new DynamoDbSinkOptions(options), 0);

        // Act & Assert
        assertDoesNotThrow(writer::abort);

    }

}
