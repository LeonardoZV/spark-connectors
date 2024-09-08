# Spark Connectors - AWS SQS Sink

[![Coverage](http://leozvasconcellos-sonarqube.eastus.azurecontainer.io:9000/api/project_badges/measure?project=com.leonardozv%3Aspark-connectors-aws-sqs&metric=coverage&token=sqb_4fe1fbef3a1eaf3986612df465565793e96e0602)](http://leozvasconcellos-sonarqube.eastus.azurecontainer.io:9000/dashboard?id=com.leonardozv%3Aspark-connectors-aws-sqs)
[![Quality Gate Status](http://leozvasconcellos-sonarqube.eastus.azurecontainer.io:9000/api/project_badges/measure?project=com.leonardozv%3Aspark-connectors-aws-sqs&metric=alert_status&token=sqb_4fe1fbef3a1eaf3986612df465565793e96e0602)](http://leozvasconcellos-sonarqube.eastus.azurecontainer.io:9000/dashboard?id=com.leonardozv%3Aspark-connectors-aws-sqs)

A custom sink provider for Apache Spark that sends the contents of a dataframe to AWS SQS.

It supports the [SQS Extended Client](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html) to manage large message payloads, from 256 KB and up to 2 GB.

The following options can be configured:
- **region** of the queue. Default us-east-2.
- **queueName** of the queue.
- **batchSize** so we can group N messages in one call. Default 10.
- **queueOwnerAWSAccountId** aws account of the sqs queue. Needed if the sqs is in a different account than the spark job. Optional argument.

SQS Extended Client options:
- **useSqsExtendedClient** if you want to use the SQS Extended Client to send messages larger than 256KB. Default false.
- **bucketName** when using the sqs extended client, you need to specify the bucket name where the messages will be stored. 
- **payloadSizeThreshold** when using the sqs extended client, you need to specify the threshold size in bytes. Default 256KB.

```java
df.write()
    .format("sqs")
    .mode(SaveMode.Append)
    .option("region", "us-east-2")
    .option("queueName", "my-test-queue")
    .option("batchSize", "10")
    .option("queueOwnerAWSAccountId", "123456789012")
    .option("useSqsExtendedClient", "true")
    .option("bucketName", "my-test-bucket")
    .option("payloadSizeThreshold", "1000")
    .save();
```

The dataframe:
- **must** have a column called **value** (string) containing the body of each message.
- **may** have a column called **msg_attributes** (map of [string, string]). Each key/value wil be add as a [metadata attribute](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html) to the SQS message.
- **may** have a column called **group_id** (string) containing the group id used by [FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html).

The folder [/spark-connectors-aws-sqs/src/test/resources](/spark-aws-messaging/src/test/resources) contains some PySpark simple examples used in the integration tests (the *endpoint* option is not required).

Don't forget you'll need to configure the default credentials in your machine before running the example. See
[Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for more information.

It also needs the *software.amazon.awssdk:sqs* package to run, so you can provide it through the *packages* parameter of spark-submit.

The following commands can be used to run the sample of how to use this library.

``` bash
# Without the extended client
spark-submit --packages com.leonardozv:spark-connectors-aws-sqs:1.0.0,software.amazon.awssdk:sqs:2.27.17 test.py sample.txt

# With the extended client
spark-submit --packages com.leonardozv:spark-connectors-aws-sqs:1.0.0,software.amazon.awssdk:sqs:2.27.17,software.amazon.awssdk:s3:2.27.17,com.amazonaws:amazon-sqs-java-extended-client-lib:2.1.1 test.py sample.txt
```

And this is the test.py file content.

``` python
import sys 
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("File: " + sys.argv[1])

    spark = SparkSession\
        .builder\
        .appName("SQS Write")\
        .getOrCreate()

    df = spark.read.text(sys.argv[1])
    df.show()
    df.printSchema()

    df.write.format("sqs").mode("append")\
        .option("queueName", "test")\
        .option("batchSize", "10") \
        .option("region", "us-east-2") \
        .save()

    spark.stop()
```
This library is available at Maven Central repository, so you can reference it in your project with the following snippet.

``` xml
<dependency>
    <groupId>com.leonardozv</groupId>
    <artifactId>spark-connectors-aws-sqs</artifactId>
    <version>1.1.0</version>
</dependency>
```

The IAM permissions needed for this library to write on a SQS queue are *sqs:GetQueueUrl* and *sqs:SendMessage*.

## Messaging delivery semantics and error handling

The sink is at least once  so some messages might be duplicated. If something wrong happens when the data is being written by a worker node, Spark will retry the task in another node. Messages that have already been sent could be sent again.

## Architecture

It's easy to get lost while understanding all the classes are needed, so we can create a custom sink for Spark. Here's a class diagram to make it a little easy to find yourself. Start at SQSSinkProvider, it's the class that we configure in Spark code as a *format* method's value.

![Class diagram showing all the classes needed to implement a custom sink](/doc/assets/Class%20Diagram-Page-1.png "Class diagram showing all the classes needed to implement a custom sink")

## How to

- [Use this library with AWS Glue](doc/aws-glue.md)