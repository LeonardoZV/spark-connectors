import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Missing parameters")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("SQS Write") \
        .getOrCreate()

    df = spark.createDataFrame([("foo",)], ["value"])

    df.show()
    df.printSchema()

    df.write \
        .format("sqs") \
        .mode("append") \
        .option("endpoint", sys.argv[1]) \
        .option("queueName", "my-test") \
        .option("useSqsExtendedClient", "true") \
        .option("s3Endpoint", sys.argv[2]) \
        .option("forcePathStyle", "true") \
        .option("bucketName", "my-bucket") \
        .option("payloadSizeThreshold", "1") \
        .option("s3KeyPrefix", "prefix/") \
        .option("s3ServerSideEncryption", "SSE-KMS") \
        .option("s3SseKmsKeyId", sys.argv[3]) \
        .save()

    spark.stop()