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

spark.sql(f"""
    MERGE INTO delta.`{table_path}` AS target
    USING (SELECT 'Alice' AS name, 35 AS age, 'USA' AS country) AS source
    ON target.name = source.name AND target.country IS NULL
    WHEN MATCHED THEN 
        UPDATE SET target.age = source.age, target.country = source.country
    WHEN NOT MATCHED THEN 
        INSERT (name, age, country) VALUES (source.name, source.age, source.country)
""")

spark.sql(f"""
    SELECT * FROM delta.`{table_path}`
""").show()

# Stop the session when done
spark.stop()

print("Success!")
