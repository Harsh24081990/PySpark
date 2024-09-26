Implementing Slowly Changing Dimensions (SCD) Type 2 using the MD5 function in Databricks involves a few key steps. Here’s a detailed guide on how to do this:

### Steps to Implement SCD Type 2 in Databricks Using MD5

#### 1. **Setup Your Environment**
- Ensure you have access to a Databricks workspace and have your data sources available (both the current dimension table and the new incoming data).

#### 2. **Load Your Data**
- Load your existing dimension table (target) and the incoming data (source) into Spark DataFrames.

```python
# Load existing dimension data
target_df = spark.read.table("your_target_table")

# Load incoming new data
source_df = spark.read.table("your_source_table")
```

#### 3. **Create MD5 Hash Columns**
- Create a new column in both DataFrames that contains the MD5 hash of the relevant columns that define the uniqueness of each row. 

```python
from pyspark.sql.functions import md5, concat_ws

# Create MD5 hash for target
target_df = target_df.withColumn("RowHash", md5(concat_ws("||", target_df["col1"], target_df["col2"], target_df["col3"])))

# Create MD5 hash for source
source_df = source_df.withColumn("RowHash", md5(concat_ws("||", source_df["col1"], source_df["col2"], source_df["col3"])))
```

#### 4. **Join Source and Target DataFrames**
- Perform a left join on the source and target DataFrames using a unique identifier (e.g., `ID`).

```python
joined_df = source_df.join(target_df, source_df["ID"] == target_df["ID"], "left")
```

#### 5. **Identify New and Updated Rows**
- Use conditions to identify new rows and updated rows based on the MD5 hash and the existence of the target rows.

```python
from pyspark.sql.functions import col, when

# Create a new DataFrame to classify the rows
result_df = joined_df.select(
    source_df["*"],
    target_df["*"],
    when(target_df["ID"].isNull(), "new"). \
    when(source_df["RowHash"] != target_df["RowHash"], "updated"). \
    otherwise("unchanged").alias("row_status")
)
```

#### 6. **Process New and Updated Rows**
- Separate the rows into new and updated DataFrames based on the `row_status`.

```python
new_rows = result_df.filter(col("row_status") == "new").select(source_df["*"])
updated_rows = result_df.filter(col("row_status") == "updated").select(source_df["*"], target_df["*"])
```

#### 7. **Update the Target Table**
- For updated rows, set the `EndDate` for the existing record in the target table and insert the new version of the record.

```python
from datetime import datetime

# Mark existing records as historical
updated_target_df = updated_rows.withColumn("EndDate", current_date()).withColumn("Current", lit(0))

# Prepare new records with current dates
new_target_records = updated_rows.withColumn("StartDate", current_date()).withColumn("EndDate", lit(None)).withColumn("Current", lit(1))

# Combine the updated and new records
final_target_df = target_df.union(new_target_records)
```

#### 8. **Write Back to the Target Table**
- Write the final DataFrame back to the target table, ensuring you have the current records updated.

```python
final_target_df.write.mode("overwrite").saveAsTable("your_target_table")
```

### Example Summary
- **Load** the existing dimension and incoming data.
- **Create MD5 hashes** for both DataFrames based on relevant columns.
- **Join** the source and target data.
- **Classify** the rows into new, updated, and unchanged.
- **Process** new and updated rows, setting `EndDate` for historical records and inserting new rows.
- **Write back** to the target table.

This approach will help you manage your SCD Type 2 implementation effectively in Databricks. If you have any specific questions or need further clarification on any step, feel free to ask!

-----------------------------------------------------------------------------------------------------------------------------

# SCD type 2 using Merge into statement:-

### Example of Using MERGE INTO for SCD Type 2

Here’s a simplified example of how you might use **MERGE INTO** for SCD Type 2 in SQL:

```sql
MERGE INTO TargetTable AS target
USING SourceTable AS source
ON target.ID = source.ID
WHEN MATCHED AND target.RowHash != source.RowHash THEN
    UPDATE SET target.EndDate = CURRENT_DATE, target.Current = 0
WHEN NOT MATCHED THEN
    INSERT (ID, Col1, Col2, StartDate, EndDate, Current)
    VALUES (source.ID, source.Col1, source.Col2, CURRENT_DATE, NULL, 1);
```
=================================================================

# SCD type 1 using Merge into statement:-
~~~
MERGE INTO TargetTable AS target
USING SourceTable AS source
ON target.ID = source.ID
WHEN MATCHED THEN
    UPDATE SET 
        target.Col1 = source.Col1,
        target.Col2 = source.Col2
WHEN NOT MATCHED THEN
    INSERT (ID, Col1, Col2)
    VALUES (source.ID, source.Col1, source.Col2);
~~~
=================================================================


