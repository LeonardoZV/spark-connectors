package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DynamoDbSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final DynamoDbClient dynamodb;
    private final List<BatchStatementRequest> statements = new ArrayList<>();
    private final int batchMaxSize;
    private final boolean treatConditionalCheckFailedAsError;
    private final int statementColumnIndex;

    public DynamoDbSinkDataWriter(int partitionId,
                                  long taskId,
                                  DynamoDbClient dynamodb,
                                  int batchMaxSize,
                                  boolean treatConditionalCheckFailedAsError,
                                  int statementColumnIndex) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.batchMaxSize = batchMaxSize;
        this.treatConditionalCheckFailedAsError = treatConditionalCheckFailedAsError;
        this.dynamodb = dynamodb;
        this.statementColumnIndex = statementColumnIndex;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        BatchStatementRequest batchStatementRequest = BatchStatementRequest.builder().statement(record.getString(statementColumnIndex)).build();
        statements.add(batchStatementRequest);
        if (statements.size() >= batchMaxSize) {
            executeStatements();
        }
    }

    @Override
    public WriterCommitMessage commit() {
        if (!statements.isEmpty()) {
            executeStatements();
        }
        return new DynamoDbSinkWriterCommitMessage(partitionId, taskId);
    }

    @Override
    public void abort() {
        // nothing to abort here, since this sink is not atomic
    }

    @Override
    public void close() {
        // nothing to close
    }

    private void executeStatements() {
        BatchExecuteStatementRequest request = BatchExecuteStatementRequest.builder().statements(statements).build();
        BatchExecuteStatementResponse response = dynamodb.batchExecuteStatement(request);
        List<BatchStatementError> errors = new ArrayList<>();
        for (int i = 0; i < response.responses().size(); i++) {
            BatchStatementResponse r = response.responses().get(i);
            if (treatConditionalCheckFailedAsError) {
                if (r.error() != null) {
                    errors.add(r.error());
                }
            } else {
                if (r.error() != null && !r.error().code().equals(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED)) {
                    errors.add(r.error());
                }
            }
        }
        if (!errors.isEmpty()) {
            throw new DynamoDbSinkBatchResultException.Builder().withErrors(errors).build();
        }
        statements.clear();
    }
}
