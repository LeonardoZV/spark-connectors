package com.leonardozv.spark.connectors.aws.sqs.write;

public class DependencyNotFoundException extends RuntimeException {

    public DependencyNotFoundException(String message) {
        super(message);
    }

    public DependencyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
