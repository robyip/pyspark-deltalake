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

# Run SQL on a named table
# I didn't get this to work
# spark.sql("DESCRIBE TABLE table").show()

# Convert relative path to absolute path
table_path = os.path.abspath("./delta/table")

print(f"Using table path: {table_path}")

# Use the absolute path in the SQL query
spark.sql(f"DESCRIBE TABLE delta.`{table_path}`").show(truncate=False)
# Stop the session when done
spark.stop()

print("Success!")