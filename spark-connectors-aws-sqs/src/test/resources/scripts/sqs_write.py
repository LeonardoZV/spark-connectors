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

    df = spark.read.text(sys.argv[1])

    df.show()
    df.printSchema()

    df.write.format("sqs").mode("append") \
        .option("sqsEndpoint", sys.argv[2]) \
        .option("queueName", "my-test") \
        .option("batchSize", "3") \
        .save()

    spark.stop()