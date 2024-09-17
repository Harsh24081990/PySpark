## Writing (saving) DF to files: -
### When saving a DataFrame to files, you can use either of the two syntax 
###.format().save() or use the .csv() method directly.
**Writing the dataframes into fiels will create multiple part files and file names also will be decided by spark. we can only specify the directory where the part files will be saved.**
#### create a datafram manually.
~~~ python
data = [(1,'harsh','good'),(2,'happy','bad')]
schema = ['id','name','prop']

df = spark.createDataFrame(data=data, schema=schema)
display(df)
~~~
--------------------------------------------------
#### Load df to a csv file in DBFS. Using .format().save()
~~~ python
path = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/df/emp'
df.write.format("csv")\
        .mode("overwrite")\
        .option("header", "true")\
        .option("delimiter", ",")\
        .save(path)
~~~
#### Load df to a csv file in DBFS Using .csv()
~~~ python
path = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/df/csv'
df.write.csv(path=path, header=True, mode='overwrite', sep=',')
~~~
#### another way of Using .csv()
~~~ python
df.write.option("header", "true")\
        .option("delimiter", ",")\
        .mode("overwrite")\
        .csv(path)
~~~
#### Read back the created file.
~~~ python
spark.read.csv(path=path, sep=',', header=True).show()  
~~~    
----------------------------------------------
###.mode(): Defines how to handle existing files:
"overwrite": Overwrites any existing files.

"append": Appends data to existing files.

"ignore": Ignores the write operation if files already exist.

"error": (default) Raises an error if files already exist.

----------------------------------------------
