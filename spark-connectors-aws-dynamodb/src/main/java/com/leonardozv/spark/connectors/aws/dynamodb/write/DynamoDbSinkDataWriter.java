package com.leonardozv.spark.connectors.aws.dynamodb.write;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DynamoDbSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final DynamoDbClient dynamodb;
    private final DynamoDbSinkOptions options;
    private final StructType schema;
    private List<BatchStatementRequest> statements = new ArrayList<>();

    public DynamoDbSinkDataWriter(int partitionId, long taskId, DynamoDbClient dynamodb, DynamoDbSinkOptions options, StructType schema) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.dynamodb = dynamodb;
        this.options = options;
        this.schema = schema;
    }

    @Override
    public void write(InternalRow row) {

        BatchStatementRequest batchStatementRequest = BatchStatementRequest.builder()
                .statement(row.getString(this.schema.fieldIndex("value")))
                .build();

        this.statements.add(batchStatementRequest);

        if (this.statements.size() >= this.options.batchSize()) {
            executeStatementsWithExponentialRandomBackoff();
        }

    }

    @Override
    public WriterCommitMessage commit() {

        if (!this.statements.isEmpty()) {
            executeStatementsWithExponentialRandomBackoff();
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



    private void executeStatementsWithExponentialRandomBackoff() {

        IntervalFunction intervalFunction = IntervalFunction
                .ofExponentialRandomBackoff(this.options.retryInitialInterval(), this.options.retryMultiplier(), this.options.retryRandomizationFactor(), this.options.retryMaxInterval());

        Set<String> retryExceptionsWithRetryableErrors = Stream
                .concat(this.options.retryExceptions().stream(), Stream.of(ResponseContainsRetryableErrorsException.class.getName()))
                .collect(Collectors.toSet());

        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(this.options.retryMaxAttempts())
                .intervalFunction(intervalFunction)
                .retryExceptions(DynamoDbSinkParsers.parseExceptions(retryExceptionsWithRetryableErrors))
                .ignoreExceptions(DynamoDbSinkParsers.parseExceptions(this.options.ignoreExceptions()))
                .build();

        Retry retry = Retry.of("executeStatements", retryConfig);

        Runnable executeFunction = Retry.decorateRunnable(retry, this::executeStatements);

        executeFunction.run();

    }

    private void executeStatements() {

        BatchExecuteStatementRequest request = BatchExecuteStatementRequest.builder()
                .statements(this.statements)
                .build();

        BatchExecuteStatementResponse response = this.dynamodb.batchExecuteStatement(request);

        List<BatchStatementRequest> retryableStatements = new ArrayList<>();

        List<BatchStatementError> nonRetryableAndNonIgnorableErrors = new ArrayList<>();

        for (int i = 0; i < response.responses().size(); i++) {

            BatchStatementResponse r = response.responses().get(i);

            if (r.error() != null) {

                if (this.options.retryErrors().contains(r.error().code().toString())) {
                    retryableStatements.add(this.statements.get(i));
                } else {
                    if (!this.options.ignoreErrors().contains(r.error().code().toString())) {
                        nonRetryableAndNonIgnorableErrors.add(r.error());
                    }
                }

            }

        }

        if (!nonRetryableAndNonIgnorableErrors.isEmpty()) {
            throw new ResponseContainsNonRetryableErrorsException.Builder()
                    .withErrors(nonRetryableAndNonIgnorableErrors)
                    .build();
        }

        this.statements = retryableStatements;

        if (!this.statements.isEmpty()) {
            throw new ResponseContainsRetryableErrorsException.Builder()
                    .withErrors(nonRetryableAndNonIgnorableErrors)
                    .build();
        }

    }

}