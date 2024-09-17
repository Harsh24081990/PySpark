## PySpark_functions_and_Methods
---------------------------------------------------------------------
### .show() ---> displays the dataframe. By default it displays 20 rows. Also it truncates the columns to 20 charecters. 
.show(100)
.show(n=100, truncate=False)  ---> displays 100 rows with full column values (full content of column)
.show(n=100, truncate=8)  ---> displays columns values upto 8 charecters.
.show(vertical=True) ---> dispalays the table in vertical manner. 

df.show() ---> It's a method.
display(df) ---> It's a function.
---------------------------------------------------------------------
### withColumn() --> It's a method in module pyspark.sql.datafram, used to Add new columns or update the columns values or datatype if column already exists. 
# Need to import col function :-  from pyspark.sql.functions import col
# Only 1 column can be added/modified using one withColumn. 
#   parameters :-
#     1. colName  --> string --> name of the new/existing column.
#     2. col --> class:Column --> Expresson for the new/existing column.
#   Returun :- 
#     class:`DataFrame` --> DataFrame with new or replaced columns.
##############################################################
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

# concatinate columns:- 
df4 = df3.withColumn('Name-Location-sal', concat_ws('-', 'name', 'location', 'salary'))
df4.show()
##############################################################
### concat_ws
"""==> concat_ws() ---> concates 2 or more columns. 
takes at least 3 parameters --> first is the separator between column, after that comes the column names which need to be concatinated. 
Example : concat_ws(' ', 'full_name', lit('Shrivastava')]"""

# Add some constant value to a column values, we can use lit and concat_ws functions like below :- 
df5 = df4.withColumn('full_name', concat_ws(' ', 'name', lit('Shrivastava')))
df5.show(truncate=False)
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

##############################################################
## Using ArrayType columns.
data = [('abc', [1,2]),('def', [3,4])]
df = spark.createDataFrame(data, ['name', 'number'])
df.show()
df1 = df.withColumn('number_1', df.number[0])\
        .withColumn('number_2', df.number[1])
df1.show()

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
---------------------------------------------
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
----------------------------------------------------------------
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
data = [(1,'harsh','M',2000),(2,'payal','F',3000),(3,'happy','',4000),\
        (1,'harsh','M',2000),(4,'gappu','F',3000),(3,'hheehh','F',5000)]
schema = ['id','name','gender','salary']
df = spark.createDataFrame(data,schema)
df.show()

# ### .alias()
# df.select(df.id.alias('emp_id'),df.name.alias('Emp_name')).show()

##############################################################

### sort()/orderBy(), asc(), desc()
df.sort(df.salary).show()
df.sort(df.salary.desc()).show()
df.orderBy(df.salary).show()

##############################################################

### filter(), where(), like()
df.filter(df.name.like('h%')).show()
df.where(df.name.like('h%')).show()
df.where(df.name == 'happy').show()
df.printSchema()
df.where("name == 'harsh'").show()
df.where((df.name == 'harsh') & (df.salary.cast('long') == 2000)).show()
df.where((df.name == 'harsh') | (df.name == 'happy')).show()

##############################################################

### distinct() , dropDuplicates()
# distinct() --> Removes duplicates rows (all columns). 
# dropDuplicates() --> Can remove duplicate rows and also can remove duplicate column values by providing selective "list of columns" for which we want to remove the duplicate values. 
df.distinct().show()
df.dropDuplicates().show()
df.dropDuplicates(['gender']).show()
df.dropDuplicates(['gender']).select('id','name','gender').show()

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

##############################################################
# To remvoe duplicate rows from the output we can use distinct() method.
print("unionAll without duplicate values")
df1.unionAll(df2).distinct().show()
##############################################################
### unionByName() ---> Used to combine 2 dataframes with difference in number of columns and names.
# It checks the column names from both the dataframes and match them to combine.
df1.unionByName(df2, allowMissingColumns = True).show()

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

#### 'selectExpr' method. 
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
##############################################################
### pivot() method

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

##############################################################
# expr() function --> when we want to pass the python code to pyspark as a string value, we can use this. 
from pyspark.sql.functions import expr

##############################################################
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
### fillna() method --> used to replace the None (In spark Null is None) values with any constant literal values or 0 or empty string. 
data = [(1,'Harsh','Male','IT',2000), \
        (2,'Happy',None,'IT',2000), \
        (3,'Gappy','Male',None,2000), \
        (4,'Sappy','Male','IT',None),]
schema = ['id','name','gender','dept','salary']
df = spark.createDataFrame(data,schema)
df.show()
===============================================================
# .fillna() --> we can provide just the "value to be displayed" in place of Null values found in all the columns, or we can give the list of columns for which Null values need to be fixed. 
df.fillna('Not known').show()
df.fillna('Not known', ['gender','dept']).show()
===============================================================
# If you want to replace the null values in numeric type column, you can either pass any numeric value to be replaced with null or you can convert the numeric column to string first then pass any string value to be replaced with null in integer column. 
df.fillna(0, ['salary']).show()

df1=df.withColumn('salary', col('salary').cast('String'))
df1.printSchema()
df1.fillna('Not known', ['gender','dept','salary']).show()

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

##############################################################
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

##############################################################
%sql --databricks magic command --> %sql
select id, upper(name) as EMP_upper_magic from employee;

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

##############################################################


