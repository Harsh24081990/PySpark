# Handling NULLs

#### use isNotNull() function. 
- Ex: Filter rows where the 'age' column is not null
`filtered_df = df.filter(col("age").isNotNull())`

### use df.na method (Used for NULL handling in spark).
```df.na.drop()```
```df.na.drop(subset=["col1", "col2"])```

- Other "na" options are as below:-

| Function              | Full Syntax                                         | Purpose                                                   |
| --------------------- | --------------------------------------------------- | --------------------------------------------------------- |
| `drop()`              | `df.na.drop()`                                      | Remove rows containing `null` values.                     |
| `drop()` with subset  | `df.na.drop(subset=["col1", "col2"])`               | Remove rows with `null` values only in specified columns. |
| `fill()`              | `df.na.fill(value)`                                 | Replace all `null` values with a given value.             |
| `fill()` with dict    | `df.na.fill({"col1": 0, "col2": "unknown"})`        | Replace `null` values per column with different values.   |
| `replace()`           | `df.na.replace(["old1", "old2"], ["new1", "new2"])` | Replace specific values (not just nulls) with new ones.   |
| `replace()` with dict | `df.na.replace({"old_val": "new_val"})`             | Replace values based on a mapping dictionary.             |

----------

# Handling duplicates
In PySpark, duplicates are removed using **`dropDuplicates()`** or **`distinct()`**.

**Example – Remove duplicates from the entire DataFrame:**

```python
df_no_dup = df.dropDuplicates()
```

or

```python
df_no_dup = df.distinct()
```

**Example – Remove duplicates based on specific columns:**

```python
df_no_dup = df.dropDuplicates(["id", "name"])
```

`dropDuplicates(["col1", "col2"])` → keeps the first occurrence and drops others with the same values in the given columns.

-------------

# Handling special / bad characters. 

You can handle special or bad characters in PySpark using **`regexp_replace()`** from `pyspark.sql.functions`.

**Example – Remove all special characters from a column:**

```python
from pyspark.sql.functions import regexp_replace

# Remove special characters (keep only letters, numbers, and spaces)
df_clean = df.withColumn(
    "name",
    regexp_replace("name", "[^a-zA-Z0-9 ]", "")
)
```

**Example – Replace special characters with an underscore:**

```python
df_clean = df.withColumn(
    "name",
    regexp_replace("name", "[^a-zA-Z0-9 ]", "_")
)
```

**Example – Keep only alphabets (remove numbers and special chars):**

```python
df_clean = df.withColumn(
    "name",
    regexp_replace("name", "[^a-zA-Z]", "")
)
```
