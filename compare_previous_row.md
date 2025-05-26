
## Problem : compare each date price with previous date price and create the price_change column accordingly.
![image](https://github.com/user-attachments/assets/2efcfa83-b889-4932-a2db-389b413d9116)

## Using self join and "date_add" function

```python
from pyspark.sql.functions import expr, col, date_add, to_date, when

# Step 1: Read the table
stocks_df = spark.table("stocks")

# Step 2: Ensure 'date' column is in date format
stocks_df = stocks_df.withColumn("date", to_date(col("date")))

# Step 3: Alias the DataFrame for self join
a = stocks_df.alias("a")
b = stocks_df.alias("b")

# Step 4: Perform LEFT JOIN with shifted date
joined_df = a.join(
    b,
    (col("a.stock_id") == col("b.stock_id")) &
    (col("a.date") == date_add(col("b.date"), 1)),
    how="left"
)

# Step 5: Apply CASE logic for price_change
final_df = joined_df.withColumn(
    "price_change",
    when(col("b.price").isNull(), None)
    .when(col("a.price") == col("b.price"), "SAME")
    .when(col("a.price") > col("b.price"), "UP")
    .when(col("a.price") < col("b.price"), "DOWN")
).select(
    col("a.stock_id"),
    col("a.date"),
    col("a.price"),
    col("price_change")
).orderBy("stock_id", "date")

# Step 6: Show the result
final_df.show()
```


## using row_num windows function
```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, when

# Step 1: Read the table
stocks_df = spark.table("stocks")

# Step 2: Add row numbers per stock_id ordered by date
window_spec = Window.partitionBy("stock_id").orderBy("date")
stocks_with_rn = stocks_df.withColumn("rn", row_number().over(window_spec))

# Step 3: Self-join the DataFrame to get previous row's price
current = stocks_with_rn.alias("current")
previous = stocks_with_rn.alias("previous")

joined_df = current.join(
    previous,
    (col("current.stock_id") == col("previous.stock_id")) &
    (col("current.rn") == col("previous.rn") + 1),
    how="left"
)

# Step 4: Calculate price_change and apply CASE logic
result_df = joined_df.withColumn(
    "price_change",
    col("current.price") - col("previous.price")
).withColumn(
    "PRICE_CHANGE",
    when(col("price_change").isNull(), None)
    .when(col("price_change") > 0, "UP")
    .when(col("price_change") < 0, "DOWN")
    .when(col("price_change") == 0, "SAME")
)

# Step 5: Select final columns
final_df = result_df.select(
    col("current.stock_id").alias("stock_id"),
    col("current.date").alias("date"),
    col("current.price").alias("price"),
    col("PRICE_CHANGE")
)

# Step 6: Show the result
final_df.show()

```

## using "lag" windows function.
``python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, when

# Step 1: Start Spark session (skip this in Databricks notebooks, already available)
# spark = SparkSession.builder.getOrCreate()

# Step 2: Read the table "stocks" from Databricks metastore
stocks_df = spark.table("stocks")

# Step 3: Define the window specification for LAG
window_spec = Window.partitionBy("stock_id").orderBy("date")

# Step 4: Calculate the price change using lag
stocks_with_change = stocks_df.withColumn(
    "price_change",
    col("price") - lag("price", 1).over(window_spec)
)

# Step 5: Apply CASE logic to interpret price movement
stocks_with_flag = stocks_with_change.withColumn(
    "PRICE_CHANGE",
    when(col("price_change").isNull(), None)
    .when(col("price_change") > 0, "UP")
    .when(col("price_change") < 0, "DOWN")
    .when(col("price_change") == 0, "SAME")
)

# Step 6: Select final required columns
final_df = stocks_with_flag.select(
    "stock_id",
    "date",
    "price",
    "PRICE_CHANGE"
)

# Step 7: Show the result
final_df.show()
```
