package com.leonardozv.spark.connectors.aws.dynamodb;


import software.amazon.awssdk.services.dynamodb.model.BatchStatementError;

import java.util.List;

public class DynamoDbSinkBatchResultException extends RuntimeException {

    public DynamoDbSinkBatchResultException(String message) {
        super(message);
    }

    public DynamoDbSinkBatchResultException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class Builder {
        private List<BatchStatementError> errors;
        public Builder withErrors(List<BatchStatementError> errors) {
            this.errors = errors;
            return this;
        }
        public DynamoDbSinkBatchResultException build() {
            String[] failedMessages = errors.stream().map(BatchStatementError::message).distinct().toArray(String[]::new);
            return new DynamoDbSinkBatchResultException("Some messages failed to be sent to the SQS queue with the following errors: [" + String.join("; ", failedMessages) + "]");
        }
    }
}
