import sys
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Missing parameters")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("DynamoDb Write") \
        .getOrCreate()

    df = spark.createDataFrame([("UPDATE \"my-table\" SET age = 31 WHERE id = '124'",)], ["statement"])

    df.show()
    df.printSchema()

    df.write \
        .format("dynamodb") \
        .mode("append") \
        .option("endpoint", sys.argv[1]) \
        .option("batchSize", "25") \
        .option("errorsToIgnore", "ConditionalCheckFailed") \
        .save()

    spark.stop()