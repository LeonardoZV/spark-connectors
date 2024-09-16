import sys
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Missing parameters")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("SQS Write") \
        .getOrCreate()

    df = spark.createDataFrame([("foo",)], ["value"])

    df.show()
    df.printSchema()

    df.write.format("sqs").mode("append") \
        .option("sqsEndpoint", sys.argv[1]) \
        .option("queueName", "my-test") \
        .option("batchSize", "3") \
        .option("useSqsExtendedClient", "true") \
        .option("s3Endpoint", sys.argv[2]) \
        .option("forcePathStyle", "true") \
        .option("bucketName", "my-bucket") \
        .option("payloadSizeThreshold", "1") \
        .save()

    spark.stop()