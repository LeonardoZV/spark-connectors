# Spark Connectors - AWS DynamoDB Sink

A custom sink provider for Apache Spark that sends the contents of a dataframe to AWS DynamoDB.

It supports the following DynamoDB APIs:
- [ExecuteBatchStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html)
- ~~TransactWriteItems~~ (TO-DO)
- ~~BatchWriteItem~~ (TO-DO)

# Getting Started

#### Importing the Connector ####

This library is available at Maven Central repository, so you can reference it in your project with the following snippet.

``` xml
<dependency>
    <groupId>com.leonardozv</groupId>
    <artifactId>spark-connectors-aws-dynamodb</artifactId>
    <version>1.0.0</version>
</dependency>
```

#### Permissioning ####

The IAM permissions needed for this library to write on DynamoDB are ?.

Don't forget you'll need to configure the default credentials in your machine. See
[Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for more information.

#### Configuration ####

The following options can be configured:
- **region** of the queue. Default us-east-2.
- **batchSize** so we can group N statements in one call. Default 25.
- **ignoreConditionalCheckFailedError** if you want the ConditionalCheckFailed error to be ignored. Default false.

```java
df.write()
    .format("dynamodb")
    .mode(SaveMode.Append)
    .option("region", "us-east-2")
    .option("batchSize", "25")
    .option("ignoreConditionalCheckFailedError", "false")
    .save();
```

The dataframe:
- **must** have a column called **statement** (string) containing the PartiQL Statement.

#### Running ####

It also needs the software.amazon.awssdk:dynamodb package to run, so you can provide it through the packages parameter of spark-submit.

The following commands can be used to run the example of how to use this library:

``` bash
spark-submit --packages com.leonardozv:spark-connectors-aws-dynamodb:1.0.0,software.amazon.awssdk:dynamodb:2.27.17 test.py sample.txt
```

And this is the test.py file content.

``` python
import sys 
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("File: " + sys.argv[1])

    spark = SparkSession\
        .builder\
        .appName("DynamoDB Write")\
        .getOrCreate()

    df = spark.read.text(sys.argv[1])
    
    df.show()
    df.printSchema()

    df.write.format("dynamodb").mode("append")\
        .option("region", "us-east-2")\
        .option("batchSize", "25")\        
        .save()

    spark.stop()
```

## Messaging delivery semantics and error handling

The sink is at least once. If something wrong happens when the data is being written by a worker node, Spark will retry the task in another node until it reaches *spark.task.maxFailures*. Statements that have already been executed could be executed again.

The ignoreConditionalCheckFailedError option can be used to ignore the ConditionalCheckFailed error and treat the execution as a success. This error is thrown when a condition specified in the statement is not met.

## How to

- [Use this library with AWS Glue](doc/aws-glue.md)