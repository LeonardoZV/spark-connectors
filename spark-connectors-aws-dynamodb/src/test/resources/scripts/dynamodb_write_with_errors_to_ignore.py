import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Missing parameters")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("DynamoDb Write") \
        .getOrCreate()

    df = spark.read.text(sys.argv[1])

    df.show()
    df.printSchema()

    df.write \
        .format("dynamodb") \
        .mode("append") \
        .option("endpoint", sys.argv[2]) \
        .option("errorsToIgnore", "ConditionalCheckFailed") \
        .save()

    spark.stop()