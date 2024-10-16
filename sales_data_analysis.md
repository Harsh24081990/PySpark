Sales data analysis using SQL --> https://github.com/Harsh24081990/sql/blob/main/sales_analysis.md
### CSV file have product_id, product_category, sales_date, sales_amount
### find out Most sold product_category in each year. 

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum, rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Most Sold Product Category") \
    .getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("path/to/retail_sales.csv", header=True, inferSchema=True)

# Calculate total sales by year and product category
yearly_sales = df.groupBy(year("sales_date").alias("sales_year"), "product_category") \
    .agg(sum("sales_amount").alias("total_sales"))

# Define the window specification for ranking
window_spec = Window.partitionBy("sales_year").orderBy(yearly_sales.total_sales.desc())

# Rank the product categories
ranked_sales = yearly_sales.withColumn("sales_rank", rank().over(window_spec))

# Filter for the most sold product category in each year
result = ranked_sales.filter(ranked_sales.sales_rank == 1)

# Show the results
result.select("sales_year", "product_category", "total_sales", "sales_rank").show()

# Stop the Spark session
spark.stop()
```
