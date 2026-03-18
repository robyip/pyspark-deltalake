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

# Updated data for overwrite
updated_data = [("Alice", 35), ("Bob", 46), ("Cathy", 30)]
columns = ["name", "age"]
updated_df = spark.createDataFrame(updated_data, columns)

# Overwrite the current contents of the Delta table
updated_df.write.format("delta").mode("overwrite").save("./delta/table")

print("Success!")