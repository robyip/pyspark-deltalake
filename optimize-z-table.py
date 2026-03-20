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

# Use an absolute path
table_path = os.path.abspath("./delta/table")

# Create the table first if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{table_path}`
    (name STRING, age INT, country STRING)
    USING delta
""")

# Use an absolute path
table_path = os.path.abspath("./delta/table")

spark.sql(f"""
   OPTIMIZE delta.`{table_path}`
    ZORDER BY (name, age);
""")

spark.sql(f"""
    SELECT * FROM delta.`{table_path}`
""").show()

# Stop the session when done
spark.stop()

print("Success!")
