# Spark Connectors - AWS SQS

A custom connector for Apache Spark that sends messages to AWS SQS.

It supports the [AWS SQS Extended Client](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html) to manage large message payloads, from 256 KB and up to 2 GB.

It currently supports the following spark operations:
- batch write operation.
- ~~batch read operation~~ (TO-DO)
- ~~streaming write operation~~ (TO-DO)
- ~~streaming read operation~~ (TO-DO)

## Getting Started

### Minimum requirements

To run the connector you will need **Java 8+** and **Spark 3.2.1+**

### Permissioning

The IAM permissions needed for this library to write on a SQS queue are *sqs:GetQueueUrl* and *sqs:SendMessage*.

The IAM permission needed for this library to write on a S3 when using the SQS Extended Client are *s3:PutObject* and *s3:ListBucket*.

Don't forget to configure the default credentials in your machine. See [Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for more information.

### Configuration

The following options can be configured in the writer:

| Option                   | Description                                                                                                                                                                                            | Required                                                  | Default                    |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|----------------------------|
| `credentialProvider`     | The credential provider to be used by the sqs client. [Credential providers available](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html). | No                                                        | DefaultCredentialsProvider |
| `profile`                | The profile to be used by the sqs client when credentialProvider is ProfileCredentialsProvider.                                                                                                        | Yes when credentialProvider is ProfileCredentialsProvider | default                    |
| `accessKey`              | The access key to be used by the sqs client when credentialProvider is StaticCredentialsProvider.                                                                                                      | Yes when credentialProvider is StaticCredentialsProvider  |                            |
| `secretKey`              | The secret key to be used by the sqs client when credentialProvider is StaticCredentialsProvider.                                                                                                      | Yes when credentialProvider is StaticCredentialsProvider  |                            |
| `sessionToken`           | The session token to be used by the sqs client when credentialProvider is StaticCredentialsProvider.                                                                                                   | Yes when credentialProvider is StaticCredentialsProvider  |                            |
| `endpoint`               | The endpoint to be used by the sqs client.                                                                                                                                                             | No                                                        |                            |
| `region`                 | The region of the queue.                                                                                                                                                                               | No                                                        | us-east-1                  |
| `queueName`              | The name of the queue.                                                                                                                                                                                 | Yes                                                       |                            |
| `batchSize`              | The number of messages to be sent in one call.                                                                                                                                                         | No                                                        | 10                         |
| `queueOwnerAWSAccountId` | The AWS account of the sqs queue. Needed if the sqs is in a different account than the spark job.                                                                                                      | No                                                        |                            |
| `useSqsExtendedClient`   | If you want to use the SQS Extended Client to send messages larger than 256KB.                                                                                                                         | No                                                        | false                      |

AWS SQS Extended Client options (to be used if useSqsExtendedClient is true):

| Option                   | Description                                                                                                                                                                                             | Required                                                    | Default                    |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|----------------------------|
| `s3CredentialProvider`   | The credential provider to be used by the s3 client. [Credential providers available](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html).   | No                                                          | DefaultCredentialsProvider |
| `s3Profile`              | The profile to be used by the s3 client when credentialProvider is ProfileCredentialsProvider.                                                                                                          | Yes when s3CredentialProvider is ProfileCredentialsProvider | default                    |
| `s3AccessKey`            | The access key to be used by the s3 client when credentialProvider is StaticCredentialsProvider.                                                                                                        | Yes when s3CredentialProvider is StaticCredentialsProvider  |                            |
| `s3SecretKey`            | The secret key to be used by the s3 client when credentialProvider is StaticCredentialsProvider.                                                                                                        | Yes when s3CredentialProvider is StaticCredentialsProvider  |                            |
| `s3SessionToken`         | The session token to be used by the s3 client when credentialProvider is StaticCredentialsProvider.                                                                                                     | Yes when s3CredentialProvider is StaticCredentialsProvider  |                            |
| `s3Endpoint`             | The endpoint to be used by the s3 client.                                                                                                                                                               | No                                                          |                            |
| `s3Region`               | The region of the bucket.                                                                                                                                                                               | No                                                          | us-east-1                  |
| `forcePathStyle`         | Force a path-style endpoint to be used where the bucket name is part of the path.                                                                                                                       | No                                                          | false                      |
| `bucketName`             | The bucket name where the messages will be stored.                                                                                                                                                      | Yes                                                         |                            |
| `payloadSizeThreshold`   | The threshold size in bytes.                                                                                                                                                                            | No                                                          | 262144                     |
| `s3KeyPrefix`            | The key prefix to be used in the s3 bucket.                                                                                                                                                             | No                                                          |                            |

Example:

```python
df.write
    .format("sqs") \
    .mode("append") \
    .option("credentialProvider", "DefaultCredentialsProvider") \
    .option("profile", "default") \
    .option("accessKey", "AKIAIOSFODNN7EXAMPLE") \
    .option("secretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY") \
    .option("sessionToken", "AQoDYXdzEJr") \
    .option("endpoint", "http://localstack:4566") \
    .option("region", "us-east-1") \
    .option("queueName", "my-test-queue") \
    .option("batchSize", "10") \
    .option("queueOwnerAWSAccountId", "123456789012") \
    .option("useSqsExtendedClient", "true") \
    .option("s3CredentialProvider", "DefaultCredentialsProvider") \
    .option("s3Profile", "default") \
    .option("s3AccessKey", "AKIAIOSFODNN7EXAMPLE") \
    .option("s3SecretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY") \
    .option("s3SessionToken", "AQoDYXdzEJr") \
    .option("s3Endpoint", "http://localstack:4566") \
    .option("s3Region", "us-east-1") \
    .option("forcePathStyle", "false") \
    .option("bucketName", "my-test-bucket") \
    .option("payloadSizeThreshold", "262144") \
    .option("s3KeyPrefix", "prefix/") \
    .save()
```

The dataframe:

- **must** have a column called **value** (string) containing the body of each message.
- **may** have a column called **msg_attributes** (map of [string, string]). Each key/value wil be add as a [metadata attribute](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html) to the SQS message.
- **may** have a column called **group_id** (string) containing the group id used by [FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html).

### Running

This library is available at maven central repository as **com.leonardozv:spark-connectors-aws-sqs:1.0.0** and can be installed in your spark cluster through the packages parameter of spark-submit.

Dependencies needed to run this library are:

- **Without the extended client:** software.amazon.awssdk:sqs
- **With the extended client:** software.amazon.awssdk:sqs, com.amazonaws:amazon-sqs-java-extended-client-lib, software.amazon.awssdk:s3

The following commands can be used to run the example of how to use this library:

``` bash
# Without the extended client
spark-submit --packages com.leonardozv:spark-connectors-aws-sqs:1.0.0,software.amazon.awssdk:sqs:2.27.17 test.py sample.txt

# With the extended client
spark-submit --packages com.leonardozv:spark-connectors-aws-sqs:1.0.0,software.amazon.awssdk:sqs:2.27.17,com.amazonaws:amazon-sqs-java-extended-client-lib:2.1.1,software.amazon.awssdk:s3:2.27.17 test.py sample.txt
```

And this is the test.py file content.

``` python
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("File: " + sys.argv[1])

    spark = SparkSession \
        .builder \
        .appName("SQS Write") \
        .getOrCreate()

    df = spark.read.text(sys.argv[1])
    
    df.show()
    df.printSchema()

    df.write \
        .format("sqs") \
        .mode("append") \
        .option("region", "sa-east-1") \
        .option("queueName", "test") \
        .save()

    spark.stop()
```

## Messaging delivery semantics and error handling

The sink is at least once. If something wrong happens when the data is being written by a worker node, Spark default behavior is to retry the task in another node until it reaches *spark.task.maxFailures*. Messages that have already been sent could be sent again, generating duplicates. That's because the spark behavior is to retry the entire batch when there are errors.

## How to

- [Use this library with AWS Glue](../docs/aws-glue.md)