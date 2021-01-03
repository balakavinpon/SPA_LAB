# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

# Create a SparkSession 
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///logs").appName("StructuredStreamingDemo").getOrCreate()

# Monitor the logs directory for new server log data, and read in the raw lines as log_lines
log_lines = spark.readStream.text("logs")

# Parse out the log format
host_exp = r'(^\S+\.[\S+\.]+\S+)\s'
time_exp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
general_exp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_exp = r'\s(\d{3})\s'
content_size_exp = r'\s(\d+)$'

logs_DF = log_lines.select(regexp_extract('value', host_exp, 1).alias('Host'),
                         regexp_extract('value', time_exp, 1).alias('Timestamp'),
                         regexp_extract('value', general_exp, 1).alias('Method'),
                         regexp_extract('value', general_exp, 2).alias('Endpoint'),
                         regexp_extract('value', general_exp, 3).alias('Protocol'),
                         regexp_extract('value', status_exp, 1).cast('integer').alias('Status'),
                         regexp_extract('value', content_size_exp, 1).cast('integer').alias('Content_size'))

# Maintain a running count of every access by a field
status_counts_DF = logs_DF.groupBy(logs_DF.Status).count()
#status_counts_DF = logs_DF.groupBy(logs_DF.Host).count()

# Start streaming query, dumping results to the console
query = ( status_counts_DF.writeStream.outputMode("complete").format("console").queryName("running_counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()
