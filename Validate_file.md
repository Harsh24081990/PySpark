Que 11: There is a .DAT file. the header contain the number of records in the file. The rows are pipe delimited. the trailer contains the date. Using pyspark validate the file by matching the number of rows in the file with the number given in the header of the file and date should be the current date. If these 2 condition are satisfied print the message "File is OK".
Ans : use 
df.first().value or df.head(1)[0].value
df.last(1)[0].value
df.count()
datetime.now().strftime('%Y-%m-%d')
if condition

```python
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id

df = spark.read.option("delimiter", "|").csv("/FileStore/path/to/your_file.DAT") \
       .withColumn("row_num", monotonically_increasing_id())

count = df.count()

header = df.orderBy("row_num").limit(1).select("_c0").first()[0]
trailer = df.orderBy("row_num", ascending=False).limit(1).select("_c0").first()[0]

if int(header) == count - 2 and trailer == datetime.now().strftime("%Y%m%d"):
    print("File is OK")
else:
    print("File validation failed")
```
