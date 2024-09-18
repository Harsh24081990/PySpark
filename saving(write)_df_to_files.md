%md
## Writing (saving) DF to files: -
### When saving a DataFrame to files, you can use either of the two syntax 
###.format().save() or use the .csv() method directly.
**Writing the dataframes into fiels will create multiple part files and file names also will be decided by spark. we can only specify the directory where the part files will be saved.**
#### create a datafram manually.
~~~
data = [(1,'harsh','good'),(2,'happy','bad')]
schema = ['id','name','prop']

df = spark.createDataFrame(data=data, schema=schema)
display(df)
~~~
--------------------------------------------------
#### Load df to a csv file in DBFS. Using .format().save()
~~~
path = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/df/emp'
df.write.format("csv")\
        .mode("overwrite")\
        .option("header", "true")\
        .option("delimiter", ",")\
        .save(path)
~~~
#### Load df to a csv file in DBFS Using .csv()
~~~
path = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/df/csv'
df.write.csv(path=path, header=True, mode='overwrite', sep=',')
~~~
#### another way of Using .csv()
~~~
df.write.option("header", "true")\
        .option("delimiter", ",")\
        .mode("overwrite")\
        .csv(path)
~~~
#### Read back the created file.
~~~
spark.read.csv(path=path, sep=',', header=True).show()  
~~~    
----------------------------------------------
###.mode(): Defines how to handle existing files:
"overwrite": Overwrites any existing files.

"append": Appends data to existing files.

"ignore": Ignores the write operation if files already exist.

"error": (default) Raises an error if files already exist.

----------------------------------------------
### Parquet Format

Parquet Files Parquet is an open source column-oriented data store that provides a variety of storage optimizations, especially for analytics workloads. It provides columnar compression, which saves storage space and allows for reading individual columns instead of entire files. It is a file format that works exceptionally well with Apache Spark and is in fact the default file format. We recommend writing data out to Parquet for long-term storage because reading from a Parquet file will always be more efficient than JSON or CSV. Another advantage of Parquet is that it supports complex types. This means that if your column is an array (which would fail with a CSV file, for example), map, or struct, you’ll still be able to read and write that file without issue. Here’s how to specify Parquet as the read format:
~~~
spark.read.format("parquet")\
.load("/data/flight-data/parquet/2010-summary.parquet").show(5)

csvFile.write.format("parquet").mode("overwrite")\
.save("/tmp/my-parquet-file.parquet")
~~~
--------------------------------------------------

