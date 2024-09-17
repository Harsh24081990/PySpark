%md
## Creatre DataFrame manually by entering hardcoded values !!
~~~
### Different Ways of Creating dataframe rows manually.

#1. List of Tuples. Each Tuple represents one row of the dataframe and it's eliments represents the columns.
data = [(1,'harsh'),(2,'happy'),(3,'payal')]

#2. List of dictionay. No need to define schema (col names) separately as keys of the dictionary becomes the column names. 
data = [{'id':1,'name':'Harsh'},
        {'id':2,'name':'Happy'},
        {'id':3,'name':'payal'}]

#3. List of Tuples where one element of the tuple is of ArrayType.
data = [('abc',[1,2,3]),('def',[4,5,6]),('ghi',[7,8,9,10])]

#4. List of Tuples where one element of the tuple is of MapType.
data = [('harsh', {'Hair':'black','eye':'brown'}),\
        ('happy', {'Hair':'brown','eye':'black'})]
---------------------------------------------------------------------------
Note : 
--> we can also provide the Tuples containing list like below, 
data= ([1,"happy"], [2,"happy"],[3,"happy"] )
However using "List of tuples" is the most commonly used. 
--> [(1,"Harsh"),]  --- Correct way best practice.
--> (1,"Harsh"),  --- Correct way
--> [1,"harsh"],  --- Correct way
--> (1,"Harsh")  --- Incorrect way
--> [1,"harsh"]  --- Incorrect way
=============================================================================
## Examples of creating datafram manually with hard coded values. 
df = spark.createDataFrame([(1,'happy'),(2,'harsh')])
df.show()
---------------------------------------------------------------------------
DATA=[(1,'happy'),(2,'harsh')]
SCHEMA=['id','name']

df = spark.createDataFrame(data=DATA, schema=SCHEMA)
df.show()

"""
Note : 
--> Since we have not defined the complete schema (name and datatypes of columns), spark automatically infers the schema based on the 'data' provided. 
--> data and schema are the keywords (parameters of the method).
"""
---------------------------------------------------------------------------
"""
For defining the datatype of the columns, we need to use "type" which is a sub-module of pyspark.sql module.
We can import all the datatypes available in 'type' module (using estrick *).
Then we can define the schema using "StructType" and "ScructField" classes.
For more info we can use help(StructType) and help(ScructField)
"""

from pyspark.sql.types import * 
DATA=[(1,'happy'),(2,'harsh')]
    
schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            ])

# another way of defining the schema is like below :-
schema = 'id Integer not null,\
          name String not null'

df = spark.createDataFrame(data=DATA, schema=schema1)
df.show()
df.printSchema()

type(schema)
---------------------------------------------------------------------------
from pyspark.sql.types import * 
help(StructType)
help(StructField)
---------------------------------------------------------------------------
## Using Dictionary
#We can also create dataframe using 'Dictionary', In that case each eliment of the dict becomes a row of the df and the Keys of each eliments becomes the column names.

data = [{'id':1, 'name':'Harsh'},
        {'id':2, 'name':'Happy'}]

df = spark.createDataFrame(data=data)
df.show()
---------------------------------------------------------------------------
## Using Array in dataframe

data = [('abc', [1,2]),('def', [3,4])]
df = spark.createDataFrame(data, ['name', 'number'])
df.show()
df1 = df.withColumn('number_1', df.number[0])\
        .withColumn('number_2', df.number[1])
df1.show()
---------------------------------------------------------------------------
~~~

