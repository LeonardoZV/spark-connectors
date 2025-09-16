# Spark Connectors - AWS DynamoDB

A custom connector for Apache Spark that executes statements in AWS DynamoDB.

It supports the following DynamoDB APIs and spark operations:
- [ExecuteBatchStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html)
- - batch write operation.
- - ~~batch read operation~~ (TO-DO)
- - ~~streaming write operation~~ (TO-DO)
- - ~~streaming read operation~~ (TO-DO)
- ~~TransactWriteItems~~ (TO-DO)
- ~~BatchWriteItem~~ (TO-DO)

## Getting Started

### Minimum requirements

To run the connector you will need **Java 8+** and **Spark 3.2.1+**

### Permissioning 

The IAM permissions needed for this library to write on DynamoDB are:

- ExecuteBatchStatement: PartiQLDelete, PartiQLInsert and PartiQLUpdate.

Don't forget to configure the default credentials in your machine. See [Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for more information.

### Configuration

The following options can be configured in the writer:

| Option                      | Description                                                                                                                                                                                                                                 | Required                                                 | Default                    |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|----------------------------|
| `credentialsProvider`       | The credential provider to be used by the dynamodb client. [Credential providers available](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html).                                 | No                                                       | DefaultCredentialsProvider |
| `profile`                   | The profile to be used by the dynamodb client when credentialProvider is ProfileCredentialsProvider.                                                                                                                                        | No                                                       | default                    |
| `accessKey`                 | The access key to be used by the dynamodb client when credentialProvider is StaticCredentialsProvider.                                                                                                                                      | Yes when credentialProvider is StaticCredentialsProvider |                            |
| `secretKey`                 | The secret key to be used by the dynamodb client when credentialProvider is StaticCredentialsProvider.                                                                                                                                      | Yes when credentialProvider is StaticCredentialsProvider |                            |
| `sessionToken`              | The session token to be used by the dynamodb client when credentialProvider is StaticCredentialsProvider.                                                                                                                                   | No                                                       |                            |
| `endpoint`                  | The endpoint to be used by the dynamodb client.                                                                                                                                                                                             | No                                                       |                            |
| `region`                    | The region to be used by the dynamodb client.                                                                                                                                                                                               | No                                                       | us-east-1                  |
| `batchSize`                 | The number of statements to be grouped in one call.                                                                                                                                                                                         | No                                                       | 25                         |
| `retryExceptions`           | Exceptions that you want to be retried separated by comma. Use the full qualified class name. [Possible exceptions](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html)                          | No                                                       |                            |
| `retryErrors`               | Errors that you want to be retried separated by comma. [Possible errors](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchStatementError.html)                                                                       | No                                                       |                            |
| `retryInitialInterval`      | The initial backoff delay, in milliseconds. It’s the wait time before the first retry after a failure and serves as the base value for subsequent exponential growth.                                                                       | No                                                       | 100                        |
| `retryMultiplier`           | The exponential growth factor. Each next delay is the previous delay multiplied by this value, e.g. nextDelay = min(prevDelay × multiplier, retryMaxInterval).                                                                              | No                                                       | 2                          |
| `retryRandomizationFactor`  | The jitter factor (typically in [0, 1]). Adds randomness of up to this proportion around the computed delay to avoid retry storms.                                                                                                          | No                                                       | 0.5                        |
| `retryMaxInterval`          | The maximum cap for any backoff delay, in milliseconds. Ensures the exponentially increasing delay never exceeds this upper bound.                                                                                                          | No                                                       | 20000                      |
| `retryMaxAttempts`          | The maximum number of attempts (including the initial call as the first attempt).                                                                                                                                                           | No                                                       | 3                          |
| `ignoreExceptions`          | Exceptions that you want to be ignored and treated as a success separated by comma. Use the full qualified class name. [Possible exceptions](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html) | No                                                       |                            |
| `ignoreErrors`              | Errors that you want to be ignored and treated as a success separated by comma. [Possible errors](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchStatementError.html)                                              | No                                                       |                            |

Example:

```python
df.write
    .format("dynamodb") \
    .mode("append") \
    .option("credentialsProvider", "DefaultCredentialsProvider") \
    .option("profile", "default") \
    .option("accessKey", "AKIAIOSFODNN7EXAMPLE") \
    .option("secretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY") \
    .option("sessionToken", "AQoDYXdzEJr") \
    .option("endpoint", "http://localstack:4566") \
    .option("region", "us-east-1") \
    .option("batchSize", "25") \
    .option("retryExceptions", "software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException, software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException") \
    .option("retryErrors", "RequestLimitExceeded, ProvisionedThroughputExceeded, ThrottlingError, InternalServerError") \
    .option("retryInitialInterval", "100") \
    .option("retryMultiplier", "2") \
    .option("retryRandomizationFactor", "0.5") \
    .option("retryMaxInterval", "20000") \  
    .option("retryMaxAttempts", "3") \ 
    .option("ignoreExceptions", "software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException, software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException") \
    .option("ignoreErrors", "ConditionalCheckFailed, DuplicateItem") \
    .save()
```

The dataframe:

- **must** have a column called **value** (string) containing the PartiQL Statement.

### Running

This library is available at maven central repository as **com.leonardozv:spark-connectors-aws-dynamodb:1.0.0** and can be installed in your spark cluster through the packages parameter of spark-submit.

Dependencies needed to run this library are:

- software.amazon.awssdk:dynamodb
- io.github.resilience4j:resilience4j-retry

The following command can be used to run the example of how to use this library:

``` bash
spark-submit --packages com.leonardozv:spark-connectors-aws-dynamodb:1.0.0,software.amazon.awssdk:dynamodb:2.27.17,io.github.resilience4j:resilience4j-retry:1.7.1 test.py sample.txt
```

And this is the test.py file content.

``` python
import sys 
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("File: " + sys.argv[1])

    spark = SparkSession \
        .builder \
        .appName("DynamoDB Write") \
        .getOrCreate()

    df = spark.read.text(sys.argv[1])
    
    df.show()
    df.printSchema()

    df.write \
        .format("dynamodb") \
        .mode("append") \
        .option("region", "sa-east-1") \
        .option("batchSize", "25") \
        .save()

    spark.stop()
```

## Messaging delivery semantics and error handling

This sink provides at-least-once delivery semantics.

This library uses resilience4j-retry to either retry or ignore exceptions and errors when calling the AWS DynamoDB API. It also applies the ExponentialRandomBackoff strategy (via IntervalFunction.ofExponentialRandomBackoff), which is fully configurable.

According to the AWS DynamoDB BatchExecuteStatement API documentation, exceptions can occur at the request level and errors at the item level (because it’s a batch API). Therefore, if you want to retry or ignore exceptions at the request level, use the retryExceptions or ignoreExceptions parameters. If you want to retry or ignore errors at the item level, use the retryErrors or ignoreErrors parameters.

On retry, the library excludes any statements that were previously executed successfully or that are configured to be ignored.

If an exception or error occurs, and it is not configured to be retried or ignored, Spark’s default behavior is to retry the **entire** task on another node until it reaches spark.task.maxFailures. Note that in this case, statements that have already been executed successfully may be executed again.

## How to

- [Use this library with AWS Glue](../docs/aws-glue.md)