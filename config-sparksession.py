from pyspark.sql import SparkSession


# Initialize a SparkSession with Delta support
spark = SparkSession.builder \
    .appName("DeltaLakePractice") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Check if Spark is working
print("Spark Session Created Successfully!")

