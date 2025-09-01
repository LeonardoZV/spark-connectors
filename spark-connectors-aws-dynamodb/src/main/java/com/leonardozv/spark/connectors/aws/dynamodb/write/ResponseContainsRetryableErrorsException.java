package com.leonardozv.spark.connectors.aws.dynamodb.write;

import software.amazon.awssdk.services.dynamodb.model.BatchStatementError;

import java.util.List;

public class ResponseContainsRetryableErrorsException extends RuntimeException {

    public ResponseContainsRetryableErrorsException(String message) {
        super(message);
    }

    public ResponseContainsRetryableErrorsException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class Builder {

        private List<BatchStatementError> errors;

        public Builder withErrors(List<BatchStatementError> errors) {
            this.errors = errors;
            return this;
        }

        public ResponseContainsRetryableErrorsException build() {
            String[] failedMessages = errors.stream().map(error -> error.code() + ": " + error.message()).distinct().toArray(String[]::new);
            return new ResponseContainsRetryableErrorsException("Some statements failed to be executed in DynamoDB with the following errors: [" + String.join("; ", failedMessages) + "]");
        }

    }

}
