from pyspark.sql import SparkSession
import time


spark = SparkSession.builder \
    .appName("MyJob") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

print("\n\n=== Reading CSV ===\n")
df = spark.read.csv("/opt/spark-data/input/data.csv", header=True, inferSchema=True)
df.show()

print("\n\n=== Writing Output ===\n")
df.write.mode("overwrite").csv("/opt/spark-data/output/result", header=True)

time.sleep(5000)