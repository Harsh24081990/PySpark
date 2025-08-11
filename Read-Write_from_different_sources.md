## **1. Auto Loader (Cloud Storage → Delta)**

```python
df = (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")                # File format
        .option("cloudFiles.inferColumnTypes", "true")     # Infer schema
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/orders_schema") # Schema evolution tracking
        .load("/mnt/raw/orders/")                          # Folder path
)

(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/orders_chk")
    .outputMode("append")
    .start("/mnt/silver/orders"))
```

---

## **2. Kafka (Streaming)**

```python
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", dbutils.secrets.get("kafka-secrets", "bootstrap-servers"))
        .option("subscribe", "orders_topic")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
)

(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/kafka_orders_chk")
    .outputMode("append")
    .start("/mnt/silver/kafka_orders"))
```

---

## **3. SQL Server (Batch)**

```python
jdbc_url = "jdbc:sqlserver://<server>.database.windows.net:1433;database=<db_name>"
username = dbutils.secrets.get("sqlserver-secrets", "username")
password = dbutils.secrets.get("sqlserver-secrets", "password")

df = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "(SELECT * FROM dbo.Customers WHERE country = 'India') AS tmp")
        .option("user", username)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", "10000")
        .option("partitionColumn", "customer_id")  # Parallel read
        .option("lowerBound", "1")
        .option("upperBound", "1000000")
        .option("numPartitions", "8")
        .load()
)

(df.write.format("delta")
    .mode("append")
    .save("/mnt/silver/customers"))
```

---

## **4. Oracle (Batch)**

```python
jdbc_url = "jdbc:oracle:thin:@//<host>:1521/<service>"
username = dbutils.secrets.get("oracle-secrets", "username")
password = dbutils.secrets.get("oracle-secrets", "password")

df = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "HR.EMPLOYEES")
        .option("user", username)
        .option("password", password)
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("fetchsize", "10000")
        .load()
)

df.write.format("delta").mode("overwrite").save("/mnt/silver/employees")
```

---

## **5. File Sources (CSV, Parquet, JSON)**

```python
# Read CSV (Folder)
df_csv = (
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/raw/csv_data/")
)

# Read Parquet
df_parquet = spark.read.parquet("/mnt/raw/parquet_data/")

# Read JSON
df_json = spark.read.json("/mnt/raw/json_data/")

# Write back
df_csv.write.mode("overwrite").parquet("/mnt/silver/csv_data")
```

---

## **6. Delta Tables (Databricks native)**

```python
# Read Delta
df = spark.read.format("delta").load("/mnt/silver/orders")

# Write Delta
df.write.format("delta").mode("append").save("/mnt/gold/orders")

# Read table by name
df = spark.table("gold.orders")
```

---

## **7. REST API (via Pandas + Spark)**

```python
import requests
import pandas as pd

url = "https://api.example.com/data"
headers = {"Authorization": f"Bearer {dbutils.secrets.get('api-secrets', 'token')}"}
response = requests.get(url, headers=headers)
pdf = pd.DataFrame(response.json())

df = spark.createDataFrame(pdf)
df.write.format("delta").mode("append").save("/mnt/silver/api_data")
```

---

## **Key Production Best Practices**

* **Never hardcode credentials** → use `dbutils.secrets.get()` or managed identities.
* Always **set `checkpointLocation`** for streaming.
* Use **partitioning** (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`) for large JDBC reads.
* Prefer **Delta format** for storage → ACID + schema evolution.
* For **incremental loads**, track watermarks or use CDC mechanisms.
* For **schema evolution**, enable:

```python
.option("mergeSchema", "true")  # while writing
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Auto Loader
```

---

