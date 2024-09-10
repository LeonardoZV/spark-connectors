package com.leonardozv.spark.connectors.aws.dynamodb;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamoDbSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final DynamoDbClient dynamodb;
    private final List<BatchStatementRequest> statements = new ArrayList<>();
    private final int batchMaxSize;
    private final Map<String, String> errorsToIgnore;
    private final int statementColumnIndex;

    public DynamoDbSinkDataWriter(int partitionId,
                                  long taskId,
                                  DynamoDbClient dynamodb,
                                  int batchMaxSize,
                                  Map<String, String> errorsToIgnore,
                                  int statementColumnIndex) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.dynamodb = dynamodb;
        this.batchMaxSize = batchMaxSize;
        this.errorsToIgnore = errorsToIgnore;
        this.statementColumnIndex = statementColumnIndex;
    }

    @Override
    public void write(InternalRow row) {
        BatchStatementRequest batchStatementRequest = BatchStatementRequest.builder().statement(row.getString(statementColumnIndex)).build();
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
            if (r.error() != null && errorsToIgnore.get(r.error().code().toString()) == null) {
                errors.add(r.error());
            }
        }
        if (!errors.isEmpty()) {
            throw new DynamoDbSinkBatchResultException.Builder().withErrors(errors).build();
        }
        statements.clear();
    }

}
