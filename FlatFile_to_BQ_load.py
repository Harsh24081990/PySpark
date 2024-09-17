############### PYSPARK ### Flat file to BQ load ############
from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("test")\
        .config("temporaryGcsBucket","gs://abc-7337771-fincrimedata-dev-exp-tmis-process-je-abc/work")\
        .getOrCreate()

bucket_name = 'abc-7337771-fincrimedata-dev-exp-tmis-process-je-abc/work'
file_name = 'stage_harsh.DAT'
dataset_name = 'abc-7337771-fincrimedata-dev.tmis_exp_stage_je_abc_dev'
table_name = 'stage_harsh2'

schema = StructType([
        StructField("column1", StringType(),True),
        StructField("column2", StringType(),True),
        StructField("column3", StringType(),True),
        StructField("column4", StringType(),True)
])
#reading from file
df = spark.read.option("multiline","true") \
                .option("delimiter", "|")\
                .option("header", "false")\
                .schema(schema)\
                .csv(f"gs://{bucket_name}/{file_name}")
df.show()

#writing into table
df.write.format("bigquery")\
        .option("temporaryGcsBucket","gs://abc-7337771-fincrimedata-dev-exp-tmis-process-je-abc/work")\
        .option("table",f"{dataset_name}.{table_name}")\
        .option("schema",schema)\
        .mode("append")\
        .save()
