# Spark Connectors - AWS DynamoDB Sink
A custom sink provider for Apache Spark that sends the contents of a dataframe to AWS DynamoDB.

It supports the following DynamoDB APIs:
- [ExecuteBatchStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html)
- ~~TransactWriteItems~~ (TO-DO)
- ~~BatchWriteItem~~ (TO-DO)

The following options can be configured:
- **region** of the queue. Default us-east-2.
- **batchSize** so we can group N statements in one call. Default 25.
- **ignoreConditionalCheckFailedError** if you want the ConditionalCheckFailed error to be ignored. Default false.

```java
df.write()
    .format("sqs")
    .mode(SaveMode.Append)
    .option("region", "us-east-2")
    .option("batchSize", "25")
    .option("ignoreConditionalCheckFailedError", "false")
    .save();
```

The dataframe:
- **must** have a column called **statement** (string) containing the PartiQL Statement.