from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load CSV to BigQuery") \
    .getOrCreate()

# Define GCS bucket and CSV file path
bucket_name = "your_bucket_name"
file_path = "gs://{}/path/to/your/file.csv".format(bucket_name)

# Read CSV file into DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Write DataFrame to BigQuery table
table_name = "your_dataset.your_table_name"  # Replace with your dataset and table name
df.write.format("bigquery") \
    .option("table", table_name) \
    .option("temporaryGcsBucket", bucket_name) \
    .mode("overwrite") \
    .save()

# Stop Spark session
spark.stop()
