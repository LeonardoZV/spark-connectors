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

| Option                 | Description                                                                                                                                                                                                   | Required                                                  | Default                    |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|----------------------------|
| `credentialProvider`   | The credential provider to be used by the dynamodb client. [Credential providers available](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html).   | No                                                        | DefaultCredentialsProvider |
| `profile`              | The profile to be used by the dynamodb client when credentialProvider is ProfileCredentialsProvider.                                                                                                          | Yes when credentialProvider is ProfileCredentialsProvider | default                    |
| `accessKey`            | The access key to be used by the dynamodb client when credentialProvider is StaticCredentialsProvider.                                                                                                        | Yes when credentialProvider is StaticCredentialsProvider  |                            |
| `secretKey`            | The secret key to be used by the dynamodb client when credentialProvider is StaticCredentialsProvider.                                                                                                        | Yes when credentialProvider is StaticCredentialsProvider  |                            |
| `sessionToken`         | The session token to be used by the dynamodb client when credentialProvider is StaticCredentialsProvider.                                                                                                     | Yes when credentialProvider is StaticCredentialsProvider  |                            |
| `endpoint`             | The endpoint to be used by the dynamodb client.                                                                                                                                                               | No                                                        |                            |
| `region`               | The region of the queue.                                                                                                                                                                                      | No                                                        | us-east-1                  |
| `batchSize`            | The number of statements to be grouped in one call.                                                                                                                                                           | No                                                        | 25                         |
| `errorsToIgnore`       | Errors that you want to be ignored and treated as a success separated by comma.                                                                                                                               | No                                                        |                            |

Example:

```python
df.write
    .format("dynamodb") \
    .mode("append") \
    .option("credentialProvider", "DefaultCredentialsProvider") \
    .option("profile", "default") \
    .option("accessKey", "AKIAIOSFODNN7EXAMPLE") \
    .option("secretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY") \
    .option("sessionToken", "AQoDYXdzEJr") \
    .option("endpoint", "http://localstack:4566") \
    .option("region", "us-east-1") \
    .option("batchSize", "25") \
    .option("errorsToIgnore", "ConditionalCheckFailed,DuplicateItem") \
    .save()
```

The dataframe:

- **must** have a column called **statement** (string) containing the PartiQL Statement.

### Running

This library is available at maven central repository as **com.leonardozv:spark-connectors-aws-sqs:1.0.0** and can be installed in your spark cluster through the packages parameter of spark-submit.

Dependencies needed to run this library are:

- software.amazon.awssdk:dynamodb

The following command can be used to run the example of how to use this library:

``` bash
spark-submit --packages com.leonardozv:spark-connectors-aws-dynamodb:1.0.0,software.amazon.awssdk:dynamodb:2.27.17 test.py sample.txt
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

The sink is at least once. If something wrong happens when the data is being written by a worker node, Spark default behavior is to retry the task in another node until it reaches *spark.task.maxFailures*. Statements that have already been executed could be executed again.

The errorsToIgnore option can be used to ignore errors and treat the execution as a success. If there are no more errors in the batch that match the ignoreError option, the entire batch will be a success and the statements will not be retried. If there are more erros in the batch that not match the ignoreError option, the entire batch will be a error and all statements will be retried (even the ones marked with the ignoreErrors option). That's because the spark behavior is to retry the entire batch when there are errors.

## How to

- [Use this library with AWS Glue](../docs/aws-glue.md)