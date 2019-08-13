from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

import LogParser

spark = SparkSession.builder.appName("Log Analyzer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logs = spark.sparkContext.textFile("access_log")

access_logs = logs.map(LogParser.parse_apache_log_line)

schema = StructType([StructField('ip_address',StringType() , True),
                    StructField('client_identd',StringType() , True),
                    StructField('user_id', StringType(), True),
                    StructField('date_time',StringType() , True),
                    StructField('method', StringType(), True),
                    StructField('endpoint', StringType(), True),
                    StructField('protocol', StringType(), True),
                    StructField('response_code', IntegerType(), True),
                    StructField('content_size', LongType(),True )])

logs_df = spark.createDataFrame(access_logs, schema)

print(" ------ Maximum Content Size ------")
print(logs_df.agg({"content_size": "max"}).collect()[0][0])

print(" ------ Average Content Size ------")
print(logs_df.agg({"content_size": "avg"}).collect()[0][0])

print(" ------ Minimum Content Size ------")
print(logs_df.agg({"content_size": "min"}).collect()[0][0])

print(" ------ Count of response codes returned ------")
print(logs_df.select("response_code").count())

print(" ------ IP Addresses that have accessed the server more than 10 times ------")
print(logs_df.groupby("ip_address").count().where("count > 10").sort("count", ascending = False).show())

print(" ------ Top end points requested by count ------")
print(logs_df.groupby("endpoint").count().sort("count", ascending = False).show())
