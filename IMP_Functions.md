## regexp_replace()

---

## regexp_extract()

---

## split() --> splits based on some "delimiter". 
syntax : 
from pyspark.sql.functions import split

split(column, pattern)
column: The name of the column (or an expression) you want to split.
pattern: A regular expression string that defines where to split.

---
## substring() --> splits or take some portion out of a string based on "Position"/"index"

---
## RLIKE (used in SQL only)

------------------------

#### Examples
Here are **example PySpark codes** for commonly needed **regex task**, using `regexp_replace` and `regexp_extract` functions:

---

## ✅ 1. **Remove Special Characters**

```python
from pyspark.sql.functions import regexp_replace

df = spark.createDataFrame([("Harsh@123#",)], ["input"])
df_clean = df.withColumn("cleaned", regexp_replace("input", "[^a-zA-Z0-9]", ""))
df_clean.show()
```

📌 Output: `Harsh123`

---

## ✅ 2. **Extract Only Digits**

```python
from pyspark.sql.functions import regexp_extract

df = spark.createDataFrame([("Order12345",)], ["input"])
df_digits = df.withColumn("digits", regexp_extract("input", "\\d+", 0))
df_digits.show()
```

📌 Output: `12345`

---

## ✅ 3. **Match or Extract Email**

```python
email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

df = spark.createDataFrame([("Contact: test_user123@example.com",)], ["input"])
df_email = df.withColumn("email", regexp_extract("input", email_regex, 0))
df_email.show()
```

📌 Output: `test_user123@example.com`

---

## ✅ 4. **Validate Date Format (`YYYY-MM-DD`)**

```python
date_pattern = r"\d{4}-\d{2}-\d{2}"

df = spark.createDataFrame([("Due on 2024-12-01",)], ["input"])
df_date = df.withColumn("date", regexp_extract("input", date_pattern, 0))
df_date.show()
```

📌 Output: `2024-12-01`

---

## ✅ 5. **Replace Multiple Spaces with One**

```python
df = spark.createDataFrame([("Hello    World   Again",)], ["input"])
df_single_space = df.withColumn("normalized", regexp_replace("input", "\\s+", " "))
df_single_space.show()
```

📌 Output: `Hello World Again`

---

## ✅ 6. Email validation Using Spark SQL
sql
Copy
Edit
-- Register the DataFrame as a temporary view first in PySpark
df.createOrReplaceTempView("email_table")

-- Then run SQL
%sql
SELECT *
FROM email_table
WHERE email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
📌 Note: In SQL, escape the backslash (\\. instead of \.).

