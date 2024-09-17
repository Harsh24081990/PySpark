%md
## Create DataFrame by reading data from files !!!
### SUMMARY ###
## LOAD FILE
filepath = "file/path/in/dbfs"
1. spark.read.csv(path=file_path, schema=schema, header=True, sep=',')
2. spark.read.load(path=filepath, format='CSV', header=True, inferSchema=False, schema=schema, delimiter=',')
3. spark.read.format('csv').option(key='header',value='true').option("delimiter", ",").option('inferSchema', 'false').schema(schema).load(path=filepath)
4.spark.read.format('csv').option('header','true').option("delimiter", ",").option('inferSchema', 'false').schema(schema).csv(filepath)
#=======================================================================

### Create Schema
## Define schema progarmatically:-
from pyspark.sql.types import *
orderSchema = StructType([
                StructField("Region", StringType() ,True),
                StructField("Country", StringType() ,True),
                StructField("OrderID", IntegerType() ,True),
                StructField("UnitPrice", DoubleType() ,True)
                ])

## Define Schema Declaratively:-
orderSchema = 'Region String not null,\
               Country String not null,\
               OrderID Integer not null,\
               UnitPrice Double'
#============================================================================  

### Note : Inside "path" we can provide multiple files paths as list. Example:-
=> spark.read.csv(path=['file1path/filename','file2path/filename'], header=True, sep=',')
--> Make sure that all the files provided in list should have the same schema.
--> Also if all the fiels are present in the same folder, then we can just give just the folder path to load all the files present in that folder, if all the files are similar. Example:-
--> spark.read.csv(path='folder/path/', header=True, sep=',')                          
#============================================================================  
### WAY 1
from pyspark.sql.types import * 

filepath = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/emp_csv.csv'
schema = 'idd integer not null,\
          namee string not null,\
          salary integer'

df = spark.read.load(path=filepath, format='CSV', header=True, inferSchema=False, schema=schema, delimiter=',')

df.show()
-------------------------------------------------------
# WAY-2
df = spark.read.csv(path='dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/emp_csv.csv', header=True, sep=',')
df.show()
df.printSchema()
--------------------------------------------------------
# WAY-3 (Using "format" with "option")
from pyspark.sql.types import * 

# We can change the schema of the dataframe by specifically defining it and using it. 
schema = StructType([
            StructField("idddd", IntegerType(), True),
            StructField("nameeeee", StringType(), True),
            StructField("salaryyyy", StringType(), True)
            ])

filepath = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/emp_csv.csv'

df = spark.read.format('csv')\
    .option('header','true')\
    .option("delimiter", ",")\
    .option('inferSchema', 'false')\
    .schema(schema)\
    .csv(filepath)

df.show()
df.printSchema()
--------------------------------------------------------
# WAY-4 (Using "format" with "option" and "load")
from pyspark.sql.types import * 

# We can change the schema of the dataframe by specifically defining it and using it. 
schema = StructType([
            StructField("idddd", IntegerType(), True),
            StructField("nameeeee", StringType(), True),
            StructField("salaryyyy", StringType(), True)
            ])

filepath = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/emp_csv.csv'

df = spark.read.format('csv')\
    .option(key='header',value='true')\
    .option("delimiter", ",")\
    .option('inferSchema', 'false')\
    .schema(schema)\
    .load(path=filepath)

df.show()
--------------------------------------------------------
## Another syntax to define the schema:-
## Instead of "StructField" we can use "add" as well. 
from pyspark.sql.types import *

file_path = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/emp_csv.csv'

schema = StructType().add("f1",IntegerType(),True)\
                     .add("f2",StringType(),True)\
                     .add("f3",StringType(),True)\
                     .add("f4",StringType(),True)

# Or we can define schema like below:-
schema = StructType().add(field='f1',data_type=IntegerType(),nullable=True)\
                     .add(field="f2",data_type=StringType(),nullable=True)\
                     .add(field="f3",data_type=StringType(),nullable=True)\
                     .add(field="ff4",data_type=StringType(),nullable=True)                    

df = spark.read.csv(path=file_path, schema=schema, header=True, sep=',')
df.show()
df.printSchema()
--------------------------------------------------------
### LOAD JSON FILE
### while loading JSON FILES use multiLine=True if json object is in multiline instead of 1 line.
1. spark.read.json(filepath, multiLine=True)

# Example : Single line json file
[{"EmployeeID":1,"Name":"Alice Smith","Department":"HR","Salary":50000},
 {"EmployeeID":2,"Name":"Bob Johnson","Department":"HR","Salary":75000}]

# Example : Multi line json file
[
    {
        "EmployeeID": 1,
        "Name": "Alice Smith",
        "Department": "HR",
        "Salary": 50000
    },
    {
        "EmployeeID": 2,
        "Name": "Bob Johnson",
        "Department": "Engineering",
        "Salary": 75000
    }
]
----------------------------------------------
## load multiline json to spark df.

filepath = 'dbfs:/FileStore/shared_uploads/harsh786harsh@gmail.com/emp.json'
df = spark.read.json(path=filepath, multiLine=True)
df.show()
----------------------------------------------
### LOAD parquet file
# Similarly we can read parquet files also which is the default format for spark files.
df = spark.read_parquet(parquet_file_path/*.parquet)  # to read all the parquet files present in the folder. 
df.show()
print(df.count())
