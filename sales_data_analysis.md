
### CSV file have product_id, product_category, sales_date, sales_amount
### find out 
- 1.cummulativ sum
- 2.monthly sum
- 3.most sold product category per year

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

------------------

## USING SQL

```sql
-- 1.cummulativ sum
-- 2.monthly sum
-- 3.most sold product category per year

--1.Cummulative sum
select product_category,
        sales_date,
        sales_amount,
        sum(sales_amount) over(partition by product_category order by sales_date) as cumm_sum
from sales_table;

-- 2.Monthly sum
select product_category,
      --sales_date,
      year(sales_date) as year,
      month(sales_date) as month,
      sum(sales_amount) as monthly_sum
from sales_table
group by product_category, year, month
ORDER BY year, month, product_category; 



-- 3.most sold product per category
with CTE AS(
select product_category,
      year(sales_date) as sales_year,
      sum(sales_amount) as sales_amount
from sales_table
group by product_category, sales_year
order by sales_year
),
CTE2 AS
(
select product_category,
      sales_year,
      sales_amount,
      Dense_Rank() over(partition by sales_year order by sales_year) as RNK
from CTE)
select * from CTE2 where RNK = 1;
```
