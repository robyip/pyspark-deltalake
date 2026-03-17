import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell"

builder = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

df.write.format("delta").mode("overwrite").save("./delta/table")
print("Success!")
