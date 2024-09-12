package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.List;

public class DynamoDbSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final DynamoDbClient dynamodb;
    private final DynamoDbSinkOptions options;
    private final List<BatchStatementRequest> statements = new ArrayList<>();

    public DynamoDbSinkDataWriter(int partitionId, long taskId, DynamoDbClient dynamodb, DynamoDbSinkOptions options) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.dynamodb = dynamodb;
        this.options = options;
    }

    @Override
    public void write(InternalRow row) {

        BatchStatementRequest batchStatementRequest = BatchStatementRequest.builder().statement(row.getString(this.options.statementColumnIndex())).build();

        this.statements.add(batchStatementRequest);

        if (this.statements.size() >= this.options.batchSize()) {
            executeStatements();
        }

    }

    @Override
    public WriterCommitMessage commit() {

        if (!this.statements.isEmpty()) {
            executeStatements();
        }

        return new DynamoDbSinkWriterCommitMessage(this.partitionId, this.taskId);

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

        BatchExecuteStatementRequest request = BatchExecuteStatementRequest.builder().statements(this.statements).build();

        BatchExecuteStatementResponse response = this.dynamodb.batchExecuteStatement(request);

        List<BatchStatementError> errors = new ArrayList<>();

        for (int i = 0; i < response.responses().size(); i++) {
            BatchStatementResponse r = response.responses().get(i);
            if (r.error() != null && this.options.errorsToIgnore().get(r.error().code().toString()) == null) {
                errors.add(r.error());
            }
        }

        if (!errors.isEmpty()) {
            throw new DynamoDbSinkBatchResultException.Builder().withErrors(errors).build();
        }

        this.statements.clear();

    }

}
