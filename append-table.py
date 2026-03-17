import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Fix JAVA_HOME to your actual Java 21 path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell"

# Build Spark session with Delta Lake support
builder = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# New data to append
new_data = [("David", 40)]
columns = ["name", "age"]
new_df = spark.createDataFrame(new_data, columns)

# Append data to the existing Delta table
new_df.write.format("delta").mode("append").save("./delta/table")

print("Success!")