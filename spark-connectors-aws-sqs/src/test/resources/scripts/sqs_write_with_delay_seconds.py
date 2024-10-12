import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Missing parameters")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("SQS Write") \
        .getOrCreate()

    data = [("value 1", 5),
            ("value 2", 5),
            ("value 3", 5),
            ("value 4", 5)]

    schema = StructType([
        StructField("value",StringType(),False),
        StructField("delay_seconds",IntegerType(),False),
    ])

    df = spark.createDataFrame(data=data,schema=schema)

    df.show()
    df.printSchema()

    df.write \
        .format("sqs") \
        .mode("append") \
        .option("endpoint", sys.argv[1]) \
        .option("queueName", "my-test") \
        .save()

    spark.stop()