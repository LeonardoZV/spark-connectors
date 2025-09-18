package com.leonardozv.spark.connectors.aws.sqs.write;

import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;

import java.util.List;

public class ResponseContainsNonRetryableErrorsException extends RuntimeException {

    public ResponseContainsNonRetryableErrorsException(String message) {
        super(message);
    }

    public static class Builder {

        private List<BatchResultErrorEntry> errors;

        public Builder withErrors(List<BatchResultErrorEntry> errors) {
            this.errors = errors;
            return this;
        }

        public ResponseContainsNonRetryableErrorsException build() {
            String[] failedMessages = errors.stream().map(error -> error.code() + ": " + error.message()).distinct().toArray(String[]::new);
            return new ResponseContainsNonRetryableErrorsException("Some messages failed to be sent to the SQS queue with the following errors: [" + String.join("; ", failedMessages) + "]");
        }

    }

}
