

---

# Example1:

```python
from pyspark.sql.functions import trim, col

def clean_names(df):
    # Step 1: Trim first_name
    df = df.withColumn("first_name", trim(col("first_name")))
    
    # Step 2: Trim last_name
    df = df.withColumn("last_name", trim(col("last_name")))
    
    # Step 3: Drop rows where first_name is null
    df = df.dropna(subset=["first_name"])
    
    # Final return
    return df
```

---

### Usage

```python
df1 = spark.read.table("bronze.customers")
df1_clean = clean_names(df1)

df2 = spark.read.table("bronze.employees")
df2_clean = clean_names(df2)
```

---
# Example2:


### **Step 1: Create a reusable function**

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import trim, col

def clean_names(df: DataFrame) -> DataFrame:
    """
    Simple reusable function to trim first_name and last_name columns
    """
    return df.withColumn("first_name", trim(col("first_name"))) \
             .withColumn("last_name", trim(col("last_name")))
```

---

### **Step 2: Use it for different tables**

```python
# Table 1
df1 = spark.read.table("bronze.customers")
df1_clean = clean_names(df1)

# Table 2
df2 = spark.read.table("bronze.employees")
df2_clean = clean_names(df2)
```
---

### Note: In Python, the return statement must be indented at the same level as the code inside the function
