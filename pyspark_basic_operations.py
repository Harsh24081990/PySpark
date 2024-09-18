##############################################################
## PySpark_functions_and_Methods
##############################################################

### .show() ---> displays the dataframe. By default it displays 20 rows. Also it truncates the columns to 20 charecters. 
.show(100)
.show(n=100, truncate=False)  ---> displays 100 rows with full column values (full content of column)
.show(n=100, truncate=8)  ---> displays columns values upto 8 charecters.
.show(vertical=True) ---> dispalays the table in vertical manner. 

df.show() ---> It's a method.
display(df) ---> It's a function.

##############################################################
##### Select and SelectExpr (Select Expressions)
##############################################################

### SELECT
data = [(1,'harsh','M',2000,'IT'),(2,'payal','F',3000,'HR'),(3,'happy','M',4000,'HR')]
schema = ['id','name','gender','salary','dept']
df = spark.createDataFrame(data,schema)
-----------------------------------------------------------------
### Multiple ways to select data froma dataframe.
df.select('*').show()
df.select('id','name').show()
df.select(df.id, df.salary).show()
df.select(['id','name']).show()           #using list
df.select(df['id'],df['gender']).show()   #using column index

from pyspark.sql.functions import col
df.select(col('id'),col('name'),col('salary')).show()       #using col function

print(df.columns)   ### This prints the list of column names to the console but returns None. 
df.select([i for i in df.columns]).show()
df.select(*df.columns).show()
-----------------------------------------------------------------

### 'selectExpr' method. 
# Another way of doing this is to use 'selectExpr' method which allows to write sql like syntax. but it does not support full SQL queries directly. Instead, selectExpr is used for column-wise transformations and selections using SQL expressions.

df5 = df3.selectExpr(
    "name",
    "education",
    "CASE WHEN education IS NOT NULL \
          THEN education \
          ELSE 'No degree found' \
     END AS education_status"
)

df5.show()
--------------------------------------
--> Another example
from pyspark.sql.functions import expr, col, column

df.select("Region", "Country").show(2)

# select using variety of ways
df.select(expr("Country"), col("Region"), column("ItemType"), df.OrderID).show(2)

# Select using the alias
df.select(
    expr("Country as NewCountry"),
    col("Region").alias("New Region"),
    column("ItemType"),
    df.OrderID,
).show(2)

# Use selectexpr
df.selectExpr("Country as NewCountry", "Region").show(2)
--------------------------------------------------------
# expr() function --> when we want to pass the python code to pyspark as a string value, we can use this. 
from pyspark.sql.functions import expr
--------------------------------------------------------
##############################################################
### Limit
##############################################################
#you might want just the top ten of some DataFrame
df.limit(5).show()
df.orderBy(expr("Country desc")).limit(6).show()

##############################################################
#### withColumn() - ADD COLUMNS / MODIFY columns
##############################################################

### withColumn() --> It's a method in module pyspark.sql.datafram, used to Add new columns or update the columns values or datatype if column already exists. 
# Need to import col function :-  from pyspark.sql.functions import col
# Only 1 column can be added/modified using one withColumn. 
#   parameters :-
#     1. colName  --> string --> name of the new/existing column.
#     2. col --> class:Column --> Expresson for the new/existing column.
#   Returun :- 
#     class:`DataFrame` --> DataFrame with new or replaced columns.
=================================================================
data = [(1,'harsh'),(2,'happy'),(2,'mani')]
columns = ['id','name']
df = spark.createDataFrame(data, schema=columns)
df.show()
#------------------------------------------------------
from pyspark.sql.functions import col, lit
df.withColumn('id', col('id').cast('Integer'))

"""or we can write like below as well :- (instead of col('col_name') function   
we can use df.col_name as well), Then no need to import "col" function. """
df.withColumn('id', df.id.cast('Integer'))

# or we can write like below as well :-
df.withColumn(colName='id', col=col('id').cast('Integer'))
#------------------------------------------------------

## updating multiple columsn together. 
df1 = df.withColumn("id", col("id") * 2)\
        .withColumn("id", col("id").cast('Integer'))\
        .withColumn('name', col('name').cast('String'))
df1.show()
df1.printSchema()
#------------------------------------------------------

# Add more columns 
# Need to import lit function from pyspark.sql.functions to add literal values in newly added columns.  
df2 = df.withColumn("salary", lit('2000').cast('Integer'))\
        .withColumn("location", lit(None))  #lit(None) creates a literal value of NULL.
df2.show()
df2.printSchema()

# Also, we can use copy any old column values to the newly added column:-
df3 = df2.withColumn("copy_salary",df2.salary)
df3.show()

# Add some constant value to a column values, we can use lit and concat_ws functions like below :- 
df3 = df2.withColumn('location', concat_ws(' ', 'location', lit('India')))
df3.show(truncate=False)

##############################################################
#### CONCAT - concatinate columns (concat_ws)
##############################################################

"""==> concat_ws() ---> concates 2 or more columns. 
takes at least 3 parameters --> first is the separator between column, after that comes the column names which need to be concatinated. 
Example : concat_ws(' ', 'full_name', lit('Shrivastava')]"""

# concatinate columns:- 
df4 = df3.withColumn('Name-Location-sal', concat_ws('-', 'name', 'location', 'salary'))
df4.show()

# Add some constant value to a column values, we can use lit and concat_ws functions like below :- 
df5 = df4.withColumn('full_name', concat_ws(' ', 'name', lit('Shrivastava')))
df5.show(truncate=False)

##############################################################
#### RENAMING COLUMNS
##############################################################

## withColumnRenamed() ---> used to rename the existing columns. 

from pyspark.sql.functions import col, lit, concat_ws

data = [(1,'harsh'),(2,'happy'),(2,'mani')]
columns = ['id','name']
df = spark.createDataFrame(data, schema=columns)
df.show()

df1 = df.withColumnRenamed('id', 'emp_id')\
        .withColumnRenamed('name', 'full_name')
df1.show()
==============================================================

#### Alias the column name
data = [(1,'harsh','M',2000),(2,'payal','F',3000),(3,'happy','',4000),\
        (1,'harsh','M',2000),(4,'gappu','F',3000),(3,'hheehh','F',5000)]
schema = ['id','name','gender','salary']
df = spark.createDataFrame(data,schema)
df.show()
----------------------------------------
df.select(df.id.alias('emp_id'),df.name.alias('Emp_name')).show()

##############################################################

## Using ArrayType columns.
data = [('abc', [1,2]),('def', [3,4])]
df = spark.createDataFrame(data, ['name', 'number'])
df.show()
df1 = df.withColumn('number_1', df.number[0])\
        .withColumn('number_2', df.number[1])
df1.show()

##############################################################

## explode() --> It creates a separate row for each eliment of the ArrayType column.
from pyspark.sql.functions import explode
df2 = df.withColumn('new_number',explode('number'))
df2.show()

##############################################################

## split() --> Used to convert comma separated values present in one "string" column to one "Array_type" column.
# It takes 2 parameters. 1. col to split 2. separater present between values. 

from pyspark.sql.functions import split

data = [(1,'harsh','java,spark,pyspark'), (2,'happy','python,aws,azure')]
schema = ['id','name','skills']
df = spark.createDataFrame(data,schema)
df.show()
display(df)

df1 = df.withColumn('skill', split('skills',','))
display(df1)
df1.printSchema()

##############################################################

## array() --> Used to convert multiple string columns to one "Array_type" column.
from pyspark.sql.functions import array
data = [(1,'harsh','python','spark'), (2,'happy','java','azure')]
schema = ['id','name','primary','secondary']

df = spark.createDataFrame(data,schema)
df.show()

df1 = df.withColumn('Skillset',array('primary','secondary'))
df1.show()
df1.printSchema()

##############################################################

### array_contains() ---> sql functtion to check if a perticular value is present or not in the array. If the value is present it returns 'true', if not then 'false'. if the array is Null, it retursn 'null'. It takes two paramets. 1. array column 2. array value. 
from pyspark.sql.functions import array_contains
df2 = df1.withColumn('Has Spark Skill', array_contains('Skillset','spark'))
df2.show()

##############################################################

### Map_type columns : It is similar to "dictionary" in python or like a json file. (whereas ArrayType column is similar to "list" in python.)

from pyspark.sql.types import StructType,StructField,StringType,MapType
from pyspark.sql.functions import explode, map_keys, map_values

data = [('harsh', {'Hair':'black','eye':'brown'}),\
        ('happy', {'Hair':'brown','eye':'black'})]

schema = StructType([StructField('name',StringType()),\
                     StructField('properties',MapType(StringType(),StringType()))])

df = spark.createDataFrame(data,schema)
print("*** DataFrame with MapType of column ***")
df.show(truncate=False)
df.printSchema()      

# explde function to get Keys and values in separate columns.
df1 = df.select('name', 'properties', explode('properties'))
#(Note : we can not use 'withColumn' method here because 'explode' function will generate 2 new columns as 'Key' and 'Value' but 'withColumn' can be passed with 1 column at a time.)
print("*** explode() function ***")
df1.show(truncate = False) 

# map_keys function to show only the keys in a separate column.
df1 = df.withColumn('keysss',map_keys('properties'))
print("*** map_keys() function ***")
df1.show(truncate = False) 

# map_values function to show only the values in a separate column.
df1 = df.withColumn('valuessss',map_keys('properties'))
print("*** map_values() function ***")
df1.show(truncate = False) 

#### create_map and map
'Maps Maps are created by using the map function and key-value pairs of columns. You then can select them just like you might select from an array'
from pyspark.sql.functions import create_map

df.select(create_map(col("Region"), col("Country")).alias("complex_map")).show(2)

# query map
df.select(map(col("Region"), col("Country")).alias("complex_map"))\
   .selectExpr("complex_map['Middle']").show(2)

##############################################################

### Row() class --> Creates a row type of objects which represents a row of a df.
### Column() class --> pyspark.sql.Column class provides various functions to work with dataframe column values. 

from pyspark.sql import Row, Column

## we can create Row class directly or using Named columns and access them using either Column Indexes or using the Column Names). 
data = Row('Harsh', 10000)
print(data)
print(data[0])
print(data[1])
print(data[0] + ' ' + str(data[1]))


data = Row(Name='Harsh', salary=10000)
print(data)
print(data.Name)
print(data.salary)
print((data.Name) + ' ' + str(data.salary))
---------------------------------------------
Output:
<Row('Harsh', 10000)>
Harsh
10000
Harsh 10000
Row(Name='Harsh', salary=10000)
Harsh
10000
Harsh 10000

##############################################################

==> .show()

==> .withColumn()
==> .withColumnRenamed()

==> lit()
==> concat_ws() 

==> explode() 
==> split()
==> array()
==> array_contains()                                        


##############################################################
#### WHEN and OTHERWISE (Conditional operations)
##############################################################

### when() and otherwise() Functions. Similar to 'CASE' statements. 
data = [(1,'harsh','M',2000),(2,'payal','F',3000),(3,'happy','',4000)]
schema = ['id','name','gender','salary']

df = spark.createDataFrame(data,schema)
df.show()
# use case --> Transform the dataframe change M to Male, F to Femal and '' to Unknown
from pyspark.sql.functions import when
df = df.select(df.id, df.name, when(df.gender=='M','Male')\
                .when(df.gender=='F','Female')
                .otherwise('Unknown').alias('Gender'), df.salary)

df.show(truncate=False)    

##############################################################
### SORT / ORDERBY the columns values 
##############################################################
"""There are two equivalent operations to do this sort and orderBy that work the exact same way. They accept both column expressions and strings as well as multiple columns. The default is to sort in ascending order:
You need to use the asc and desc functions if operating on a column. These allow you to specify the order in which a given column should be sorted.
Use asc_nulls_first, desc_nulls_first, asc_nulls_last, or desc_nulls_last to specify where you would like your null values to appear in an ordered DataFrame."""

### sort()/orderBy(), asc(), desc()
df.sort(df.salary).show()
df.sort(df.salary.desc()).show()
df.orderBy(df.salary).show()

df.sort("Country").show(5)
df.orderBy("Country", "UnitsSold").show(5)
df.orderBy(col("ItemType"), col("UnitPrice")).show(5)

# asc and desc function
from pyspark.sql.functions import desc, asc

df.orderBy(expr("Country desc")).show(2)
df.orderBy(col("Country").desc(), col("UnitPrice").asc()).show(2)

##############################################################
### FILTER out the rows / WHERE
##############################################################

#Filter Functions
df2= df.filter(df.Country != "Libya")
df2.show()

#Multiple Condition
df2= df.filter((df.Country  == "Libya") |  (df.Country  == "Japan"))
df2.show()

#List of values filter
li=["Libya","Japan"]
df.filter(df.Country.isin(li)).show()

#Like Filter
df.filter(df.Country.like("%J%")).show()

#Regular Expression Filter
df.filter(df.Country.rlike("(?i)^*L$")).show()

#Assuming that one of the column in dataframe is Array then You can run filter using the below code
from pyspark.sql.functions import array_contains
df.filter(array_contains(df.phoneNumbers,"123")).show()

### filter(), where(), like()
df.filter(df.name.like('h%')).show()
df.where(df.name.like('h%')).show()
df.where(df.name == 'happy').show()
df.printSchema()
df.where("name == 'harsh'").show()
df.where((df.name == 'harsh') & (df.salary.cast('long') == 2000)).show()
df.where((df.name == 'harsh') | (df.name == 'happy')).show()
df.filter(col("UnitsSold") < 1000).show(2)
df.where("UnitsSold < 1000").show(2)
df.filter(df.UnitsSold < 1000).show(2)

##############################################################
### REMOVE DUPLICATES / DISTINCT / UNIQUE Values
##############################################################

### distinct() , dropDuplicates()
# distinct() --> Removes duplicates rows (all columns). 
# dropDuplicates() --> Can remove duplicate rows and also can remove duplicate column values by providing selective "list of columns" for which we want to remove the duplicate values. 
df.distinct().show()
df.dropDuplicates().show()
df.dropDuplicates(['gender']).show()
df.dropDuplicates(['gender']).select('id','name','gender').show()

#extract how many unique or distinct values in a DataFrame.
df.select("Country", "Region").distinct().count()
df.select("Country").distinct().count()

##############################################################
### UNION
##############################################################

### union() and unionAll() --> Both just combines the dataframes with similar schema. Both don't remove the duplicate rows. 
# This will work even if the name of columns are different beacuse it matches based on column indexes, but the number of columsn should be same.
#(However in db sql, UNION clause removes the duplicate rows from the final output)

data1 = [(1,'harsh','M',2000),(2,'payal','F',3000),(3,'happy','',4000),\
        (1,'harsh','M',2000),(4,'gappu','F',3000),(5,'hheehh','F',5000)]

data2 = [(1,'harsh','M',2000),(2,'payal','F',3000),(6,'happsdfsy','M',9000),\
        (7,'fff','F',2000),(8,'mmm','M',33),(9,'jjj','F','Ten')]        

schema1 = ['id','name','gender','salary']
schema2 = ['id','name','gender','age']

df1 = spark.createDataFrame(data1,schema1)
df1.show()
df2 = spark.createDataFrame(data2,schema2)
df2.show()

# combining these dataframes using union and unionAll. 
df1.union(df2).show()
df1.unionAll(df2).show()

# another example
df1 = df.filter("Country = 'Libya'")
df2 = df.filter("Country = 'Canada'")
df1.union(df2).show()

=============================================================
# To remvoe duplicate rows from the output we can use distinct() method.
print("unionAll without duplicate values")
df1.unionAll(df2).distinct().show()
=============================================================

### unionByName() ---> Used to combine 2 dataframes with difference in number of columns and names.
# It checks the column names from both the dataframes and match them to combine.
df1.unionByName(df2, allowMissingColumns = True).show()

##############################################################
########## Aggregate Function ################################
##############################################################

from pyspark.sql.functions import count, countDistinct
df.select(count("Region")).show()

# countDistinct
df.select(countDistinct("Region")).show()
# -- in SQL
# SELECT COUNT(DISTINCT *) FROM DFTABLE

# first and last
# You can get the first and last values from a DataFrame by using these two obviously named functions.

# in Python
from pyspark.sql.functions import first, last
df.select(first("Region"), last("Region")).show()

# min and max
# To extract the minimum and maximum values from a DataFrame, use the min and max functions:
from pyspark.sql.functions import min, max
df.select(min("UnitsSold"), max("UnitsSold")).show()

# sum
# Another simple task is to add all the values in a row using the sum function:
from pyspark.sql.functions import sum
df.select(sum("UnitsSold")).show()

# sumDistinct
# In addition to summing a total, you also can sum a distinct set of values by using the sumDistinct function:
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("UnitsSold")).show()

# avg
from pyspark.sql.functions import avg
df.select(avg("UnitsSold")).show()

# Variance and Standard Deviation for ppulation and sample
from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp

df.select(
    var_pop("Quantity"),
    var_samp("Quantity"),
    stddev_pop("Quantity"),
    stddev_samp("Quantity"),
).show()

##############################################################
#################### Grouping ################################
##############################################################
'This returns another DataFrame and is lazily performed. We do this grouping in two phases. First we specify the column(s) on which we would like to group, and then we specify the aggregation(s). The first step returns a RelationalGroupedDataset, and the second step returns a DataFrame'

df.groupBy("Region", "Country").count().show()

# in SQL
# SELECT count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId

# in Python
# Rather than passing that function as an expression into a select statement, we specify it as within agg. This makes it possible for you to pass-in arbitrary expressions that just need to have some aggregation specified. You can even do things like alias a column after transforming it for later use in your data flow:

from pyspark.sql.functions import count

df.groupBy("Region").agg(
    count("UnitsSold").alias("quan"), expr("count(UnitsSold)")
).show()

##############################################################
#### GROUPING and AGGREGATION
##############################################################

### groupBy() --> Making groups by putting similar values of the column(s) together and displaying one row to represent each group. Any aggregate function (one at a time) need to be used after groupBy('col')

data = [(1,'harsh','M',2000,'IT'),(2,'payal','F',3000,'HR'),(3,'happy','M',4000,'HR'),\
        (4,'harsh','M',20000,'sales'),(4,'gappu','F',3500,'IT'),(5,'hheehh','F',5000,'sales')]
schema = ['id','name','gender','salary','dept']   
df = spark.createDataFrame(data,schema)
df.show() 
df.groupBy('dept').count().show()
df.groupBy(df.dept,df.gender).count().show()
df.groupBy('dept','gender').count().show()
df.groupBy(df.dept).count().show()
df.groupBy('dept').max('salary').show()
=====================================================================
### groupBy().agg() --> Used to perform multiple aggregations on the gouped data together. also used to count the specific column values. 
from pyspark.sql.functions import count,sum,min,max

df.groupBy('dept').agg(count('name')).show()
df.groupBy('dept').agg(count('name').alias('No. of emp')).show()

df.groupBy('dept').agg(sum('salary').alias('Totalsalary'),\
                       count('*').alias('No of emp'),\
                       max('salary').alias('max salary'),\
                       min('salary').alias('min salary')).show()
=====================================================================
"""NOTE :- Aggregate functions used without .agg() functions are part of dataFrame class. so no need to import them.
But, Aggregate functions used with .agg() function are part of pyspark.sql.functions module and need to import them before using."""                         
=====================================================================

##############################################################
#### JOINS
##############################################################

### JOINS of Dataframes
data1 = [(1,'harsh','M',2000,'IT','Null'),(2,'payal','F',3000,'HR','Raaj'),(3,'Raaj','M',4000,'HR','Shiv'),(4,'Shiv','M',4000,'','Ram')]
schema1 = ['emp_id','name','gender','salary','dept','manager']

data2=[(1,'bsc'),(2,'ba'),(4,'BE')]
schema2 = ['id','education']

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)

# 'inner'
df1.join(df2, df1.emp_id == df2.id, 'inner').show()
# 'left'
df1.join(df2, df1.emp_id == df2.id, 'left').show()
# 'right'
df1.join(df2, df1.emp_id == df2.id, 'right').show()
# 'full'
df1.join(df2, df1.emp_id == df2.id, 'full').show()
# 'leftsemi' --> similar to left join. But displays only the columns from left table for the matching rows. 
df1.join(df2, df1.emp_id == df2.id, 'leftsemi').show()
# 'leftanti' --> Displays only the columns from left table for the NON matching rows. (i.e. rows which were not displayed in the 'leftsemi' join)
df1.join(df2, df1.emp_id == df2.id, 'leftanti').show()
=======================================================================
# self join usages.
data = [(1,'Harsh',''),(2,'Happy',1),(3,'Payal',2)]
schema = ['id','name','mgr_id']
df = spark.createDataFrame(data,schema)

from pyspark.sql.functions import col

df_self = df.alias('df_emp').join(df.alias('df_mgr') \
                  ,col('df_emp.mgr_id') == col('df_mgr.id') \
                  ,'left')\
   .select(col('df_emp.name').alias('Emp_name'), col('df_mgr.name').alias('Manager'))

df_self.show()
=============================================================
                
#Find all the employess name with their educational qualification.
df1.join(df2, df1.emp_id == df2.id, 'left').select(df1.name,df2.education).show()
#use of when() and othersise() dunction
from pyspark.sql.functions import when
from pyspark.sql.functions import col
df3 = df1.join(df2, df1.emp_id == df2.id, 'left').select(df1.name,df2.education)

df4 = df3.select(df3.name, df3.education, when(col('education').isNotNull(), col('education'))\
                .otherwise('No degree found').alias('Education_status'))

df4.show()   

##############################################################
#### Repartition and Coalesce
##############################################################
"""
Repartition will incur a full shuffle of the data, regardless of whether one is necessary. This means that you should typically only repartition when the future number of partitions is greater than your current number of partitions or when you are looking to partition by a set of columns:

Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions. This operation will shuffle your data into five partitions based on the destination country name, and then coalesce them (without a full shuffle)
"""
df.rdd.getNumPartitions()
df.repartition(5)

# If you know that you’re going to be filtering by a certain column often, it can be worth repartitioning based on that column:
df.repartition(5, col("Country"))
df.repartition(5, col("Country")).coalesce(2)

##############################################################
#### Coalesce
##############################################################

'Spark includes a function to allow you to select the first non-null value from a set of columns by using the coalesce function'

from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()

##############################################################
#### String Functions
##############################################################

from pyspark.sql.functions import initcap, lower, upper
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim

# Translate the first letter of each word to upper case in the sentence.
df.select(initcap(col("Country"))).show()
df.select(lower(col("Country"))).show()
df.select(upper(col("Country"))).show()

# To concat
display(df.select(concat(col("Region"), col("Country"))))

# To concat with separater
display(df.select(concat_ws("|", col("Region"), col("Country"))))

# instr(str, substr) - Returns the (1-based) index of the first occurrence of substr in str
display(df.select(instr(col("Region"), "Mi")))

# length(expr) - Returns the character length of string data or number of bytes of binary data
display(df.select(length(col("Region"))))

df.select(
    ltrim(lit(" HELLO ")).alias("ltrim"),
    rtrim(lit(" HELLO ")).alias("rtrim"),
    trim(lit(" HELLO ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp"),
).show(2)

-------------------------------------------------------------
from pyspark.sql import functions as F

df2 =df.select("Region","Country",F.when(df.UnitsSold > 2000, 1).otherwise(0))
df2.show()

#check isin
df2 = df[df.Country.isin("Libiya" ,"Japan")]
df2.show()

#Like
df2 = df.select("Region","Country", df.Country.like("L" ))
df2.show()

#StartsWith
df2 = df.select("Region","Country", df.Country.startswith("L"))
df2.show()
 
df2 = df.select("Region","Country", df.Country.endswith("L"))
df2.show()

##############################################################
#### Regular Expression
##############################################################

from pyspark.sql.functions import regexp_replace
regex_string = "Hello|WHITE|RED|GREEN|BLUE"
df.select(regexp_replace(col("Country"), regex_string, "COLOR").alias("color_clean"),col("Description")).show(2)

##############################################################
#### Date Handling in Spark
##############################################################

'Spark will not throw an error if it cannot parse the date; rather, it will just return null'
from pyspark.sql.functions import current_date, current_timestamp

dateDF = (
    spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
)

display(dateDF)

# Add Subtract dates
from pyspark.sql.functions import date_add, date_sub

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

# Days and Month difference between dates
from pyspark.sql.functions import datediff, months_between, to_date

dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(
    datediff(col("week_ago"), col("today"))
).show(1)

dateDF.select(
    to_date(lit("2016-01-01")).alias("start"), to_date(lit("2017-05-22")).alias("end")
).select(months_between(col("start"), col("end"))).show(1)

# incorrect date format
dateDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)

# Give date format
from pyspark.sql.functions import to_date

dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"),
)
cleanDateDF.show()

# Handle timestamp to date  format casting
from pyspark.sql.functions import (
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    minute,
    second,
)

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

# get year using the year function with date and timepstamp
cleanDateDF.select(year(to_timestamp(col("date"), dateFormat))).show()

# get month using the month function with date and timepstamp
cleanDateDF.select(month(to_timestamp(col("date"), dateFormat))).show()

# get dayofmonth using the dayofmonth function with date and timepstamp
cleanDateDF.select(dayofmonth(to_timestamp(col("date"), dateFormat))).show()

# get hour using the hour function with date and timepstamp
cleanDateDF.select(hour(to_timestamp(col("date"), dateFormat))).show()

# get minute using the minute function with date and timepstamp
cleanDateDF.select(minute(to_timestamp(col("date"), dateFormat))).show()

# get second using the second function with date and timepstamp
cleanDateDF.select(second(to_timestamp(col("date"), dateFormat))).show()

##############################################################
### ifnull, nullIf, nvl, and nvl2
##############################################################
'There are several other SQL functions that you can use to achieve similar things. ifnull allows you to select the second value if the first is null, and defaults to the first. Alternatively, you could use nullif, which returns null if the two values are equal or else returns the second if they are not. nvl returns the second value if the first is null, but defaults to the first. Finally, nvl2 returns the second value if the first is not null; otherwise, it will return the last specified value'

%sql
/*SELECT
ifnull(null, 'return_value'),
nullif('value', 'value'),
nvl(null, 'return_value'),
nvl2('not_null', 'return_value', "else_value")
FROM dfTable LIMIT 1*/

##############################################################
### PIVOT
##############################################################

# pivot() method
data = [(1,'harsh','male','IT'),\
        (2,'payal','female','HR'),\
        (3,'shanky','male','Marketing'),\
        (4,'akahnsha','female','sales'),\
        (5,'ramu','male','Admin'),\
        (6,'dinesh','male','IT'),\
        (7,'mayu','female','HR'),\
        (8,'jayati','female','IT'),\
        (9,'rahul','male','Marketing'),\
        (10,'prashant','male','Admin')]

schema = ['id','name','gender','dept']

df = spark.createDataFrame(data,schema)
df.show()

df.groupby('dept','gender').count()
df.show()
df.groupBy('dept').pivot('gender').count().show() ## converts all the distinct (grouped) values into new columns. 
df.groupBy('dept').pivot('gender',['male']).count().show() ## Can display only selective groups as new columns. 
df.groupBy('dept').pivot('gender',['male','female']).count().show()

=======================================================================

### Unpivoting in pyspark --> No direct funciton is available for unpivoting in pyspark. we can use stack() fucntion to achive this. It takes 3 arguments:-
# 1. no. of col to be converted in rows 
# 2. value to be disply in rows, current col name...........
# 3. as (col1, col2) 

data = [('HR',2,8),('IT',10,7),('Sales',2,1),('marketing',1,0)]
schema = ['dept','Male','Female']

df = spark.createDataFrame(data,schema)
df.show()

# Now we want to convert this into --> 'dept', 'gender', 'count'

df1 = df.select('dept',expr("stack(2, 'M',Male,'F',Female) as(Gender,Count)"))
df1.show()

##############################################################
#### FILLNA and FILL
##############################################################

### fillna() method --> used to replace the None (In spark Null is None) values with any constant literal values or 0 or empty string. 
data = [(1,'Harsh','Male','IT',2000), \
        (2,'Happy',None,'IT',2000), \
        (3,'Gappy','Male',None,2000), \
        (4,'Sappy','Male','IT',None),]
schema = ['id','name','gender','dept','salary']
df = spark.createDataFrame(data,schema)
df.show()
---------------------------------------------------------------
# .fillna() --> we can provide just the "value to be displayed" in place of Null values found in all the columns, or we can give the list of columns for which Null values need to be fixed. 
df.fillna('Not known').show()
df.fillna('Not known', ['gender','dept']).show()
---------------------------------------------------------------
# If you want to replace the null values in numeric type column, you can either pass any numeric value to be replaced with null or you can convert the numeric column to string first then pass any string value to be replaced with null in integer column. 
df.fillna(0, ['salary']).show()

df1=df.withColumn('salary', col('salary').cast('String'))
df1.printSchema()
df1.fillna('Not known', ['gender','dept','salary']).show()
===============================================================
# Fill
'Fill: Using the fill function, you can fill one or more columns with a set of values. This can be done by specifying a map—that is a particular value and a set of columns'

# For example, to fill all null values in columns of type String, you might specify the following:
df.na.fill("All Null values become this string")
df.na.fill("all", subset=["Country", "Region"])

fill_cols_vals = {"UnitsSold": 5, "Region": "No Value"}
df.na.fill(fill_cols_vals)

##############################################################
### Drop
##############################################################
'Drop is the he simplest function to remove rows that contain nulls. The default is to drop any row in which any value is null'

df.na.drop()

# Specifying "any" as an argument drops a row if any of the values are null. Using “all” drops the row only if all values are null or NaN for that row:
df.na.drop("any")
df.na.drop("all")

# We can also apply this to certain sets of columns by passing in an array of columns:
df.na.drop("all", subset=["Country", "Region"])

##############################################################
### isNull and isNotNul
##############################################################
# isNull
'it is a method that you can use on a Column object to check for null values in a specific column'
data = [("Alice", 30), ("Bob", None), ("Charlie", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Filter rows where the 'age' column is null
null_df = df.filter(col("age").isNull())

# Show the result
null_df.show()
---------------------------------------------------------------
# isNotNul method
'In PySpark, isNotNull is a method that you can use on a Column object. It is typically used in DataFrame operations to filter out rows with null values in a specific column'

data = [("Alice", 30), ("Bob", None), ("Charlie", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Filter rows where the 'age' column is not null
filtered_df = df.filter(col("age").isNotNull())

# Show the result
filtered_df.show()

##############################################################
#### RANGE
##############################################################

### range() method ---> used to generate 'id' columns for any given range.
df = spark.range(start=1, end=100)
df = spark.range(1,100)
df.show()

### sample() method --> To generate a small dataframe out of a huge data frame. Used for analysis or testing purpose. 
df = spark.range(1,100)

df.sample(fraction = 0.1)
display(df)

##############################################################
### COLLECT
##############################################################

### collect() method. ---> Brings all the data to driver node. It retrieve all the rows of a DataFrame and return them as a list of Row objects to the driver program.
# collect() is an ACTION so it does not return and dataframe instead it returns the data as a list of rows to the driver. 
# Use collect() with smaller dataframes. With big dataframes it may result in out of memory error as it returns the entire data to a single node.
data = [(1,'Harsh','Male','IT',2000), \
        (2,'Happy',None,'IT',2000), \
        (3,'Gappy','Male',None,2000), \
        (4,'Sappy','Male','IT',None),]

schema = ['id','name','gender','dept','salary']
df = spark.createDataFrame(data,schema)

df1 = df.collect()

print(df1)
print(df1[0])
print(df1[0][0])
print(df1[0][0], df1[0][1])
print(df1[0].id, df1[0].name)

##############################################################
#### Transform DF
##############################################################

### df.transform() method --> Used to apply any customised (or any) transformation on a dataframe. It returns a new dataframe after applying the logic. 
data = [(1,'Harsh','Male','IT',2000), \
        (2,'Happy',None,'IT',2000), \
        (3,'Gappy','Male',None,2000), \
        (4,'Sappy','Male','IT',None),]

schema = ['id','name','gender','dept','salary']
df = spark.createDataFrame(data,schema)
# Example -- Changing the Names to upper case and Increasing the salary to double.

from pyspark.sql.functions import upper
def change_to_upper (df):
    return df.withColumn('NAME',upper('name'))

def double_sal (df):
    return df.withColumn('SALARY', df.salary * 2)

df1 = df.transform(change_to_upper).transform(double_sal)
df1.show()

# or we can create a single function for doing multiple thing
def do_tranform (df):
    df1 = df.withColumn('NAME',upper('name'))
    df2 = df1.withColumn('SALARY', df.salary * 2)
    return df2

df3 = df.transform(do_tranform)
df3.show()

======================================================================

### pyspark.sql.functions.transform() Function ---> Similar to transform() method but it's useful only where the column type (on which we need to do some transformation) is an "arrayType" column.

data = [(1,'Harsh',['java','python','spark']), (2,'Happy',['python','azure'])]
schema = ['id','name','skill_set']
df = spark.createDataFrame(data,schema)
df.printSchema()

# Change the sill_set to upper case
from pyspark.sql.functions import transform, upper

def arrayToUpper(x):
    return upper(x)
df1 = df.select('id','name',transform('skill_set',arrayToUpper).alias('sill_set_upper'))
df1.show(truncate=False)

# we can also use lambda function directly without defining the def. 
df2 = df.select('id','name', transform('skill_set', lambda i: upper(i)).alias('skill array upper'))
df2.show()

##############################################################
#### createOrReplaceTemView
##############################################################

### df.createOrReplaceTemView() ---> This method is used for creating or replacting temporary db table like structure on top of the dataframe withing the 'spark session'. It allows user to write exactly SQL like syntaxes to perform any tranformation on the dataframes.
data = [(1,'harsh','male','IT'),\
        (2,'payal','female','HR'),\
        (3,'shanky','male','Marketing'),\
        (4,'akahnsha','female','sales'),\
        (5,'ramu','male','Admin'),\
        (6,'dinesh','male','IT'),\
        (7,'mayu','female','HR'),\
        (8,'jayati','female','IT'),\
        (9,'rahul','male','Marketing'),\
        (10,'prashant','male','Admin')]

schema = ['id','name','gender','dept']
df = spark.createDataFrame(data,schema)

df.createOrReplaceTempView('employee')

df1 = spark.sql("select id, upper(name) as EMP_upper from employee")
df1.show()


************************************************************
%sql --databricks magic command --> %sql
select id, upper(name) as EMP_upper_magic from employee;
************************************************************

##############################################################
#### createOrReplaceGlobalTempView
##############################################################

### df.createOrReplaceGlobalTempView() ---> similar to df.createOrReplaceTemView() method, But the view created is accessible acorss session withing the same cluster. Example. If we create 2 different notebooks withing the same cluster, both will have different session ids, however view created in one notebook can be accessed from other notebook.
data = [(1,'harsh','male','IT',3000),\
        (2,'payal','female','HR',2000)]

schema = ['id','name','gender','dept','salary']
df = spark.createDataFrame(data,schema)

df.createOrReplaceGlobalTempView('emp_global')

# for accessing this view we need to use "global_temp" along with the table name. This is namespace (db) for the global tables. 
--------------------------------------------------------------
%sql
select * from global_temp.emp_global;
--------------------------------------------------------------
# For seeing the list of all global temp tables present :-
spark.catalog.currentDatabase()  # --> it lists the current session database. 

spark.catalog.listTables('default')              #--> Retursn [list]
spark.catalog.listTables('global_temp')          #--> Retursn [list]
spark.catalog.dropTempView('employee')           #--> Returns True or False.
spark.catalog.dropGlobalTempView('emp_global')   #--> Returns True or False.
--------------------------------------------------------------

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#


