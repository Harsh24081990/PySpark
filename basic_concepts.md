## Basic concepts
#### SparkSession
The SparkSession is a "class" which is defined in the pyspark.sql module.
SparkSession is a class provided by PySpark. It is the entry point to programming Spark with the Dataset and DataFrame API. It is used for reading data, creating DataFrames, and executing SQL queries. It allows you to configure Spark settings and access different components like Spark SQL, DataFrame, and Spark Streaming.

#### spark
"spark" is commonly used as a variable name for an instance (object) of the SparkSession class. It represents the active Spark session in your application and is used to interact with Spark's functionality.<br>

To see all the methods of SparkSession class: -
~~~
dir(spark) # display all the methods of this "spark" object. 
help(spark) # displays the complete documentation of "spark" object.
~~~

#### Create a SparkSession object (spark) if not already available :- 
(In databricks cluster "spark" object is already created and ready to use)

##### Syntax
~~~ python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()
~~~

#### PySpark DataFrame
DataFrame" is a class defined in pyspark.sql module. 
Methods defined in the DataFrame class allow you to perform operations like select, filter, groupBy, agg, etc on the dataframe. <br>
These methods can be accessed by creating a object of this class using --> spark.createDataFrame() 

#### PySpark SQL Functions
"pyspark.sql.functions" is a module that contains a collection of functions used for performing operations on DataFrame. <br>
These functions can be accessed by importing them using -->  from pyspark.sql.functions import *

#### Note:-
In PySpark, DataFrames are indeed immutable, which means that any transformation or operation on a DataFrame results in the creation of a new DataFrame. The original DataFrame remains unchanged.
If you assign a new DataFrame to the same variable name as the previous df, The variable name starts pointing to the new df. <br>
The old DataFrame instances that are no longer referenced will eventually be cleaned up by Python's garbage collector, assuming there are no other references to them. Once you overwrite a DataFrame with a new instance, the previous DataFrame is no longer accessible unless it was explicitly stored elsewhere. <br>
In Python, garbage collection is handled automatically by the Python runtime, and there is no fixed time interval for when an object, including a PySpark DataFrame, will be cleaned up. Instead, garbage collection is triggered based on specific conditions. <br>
Periodic Checks: The cyclic garbage collector runs periodically, and its frequency can be influenced by the size of the program, the amount of garbage generated, and other factors.


