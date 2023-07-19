#! /usr/bin/env python
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("MRFatalLogEntriesMonMcCkIntJob") \
    .getOrCreate()

# Load the CSV file into Spark DataFrame
csv_file_path = "hdfs://path/to/BGL.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Parse the date and extract the required fields
def parse_date_and_time(date_str, time_str):
    full_date_str = f"{date_str} {time_str}"
    return datetime.datetime.strptime(full_date_str, "%Y.%m.%d %H:%M:%S")

parse_date_and_time_udf = spark.udf.register("parse_date_and_time", parse_date_and_time)

# Apply the date parsing function and select the relevant columns
df = df.withColumn("date", parse_date_and_time_udf(col("date"), col("time")))
df = df.select("date", "level", "message_content")

# Filter the DataFrame based on the specified criteria
filtered_entries = df.filter((col("date").dt.dayofweek == 2) & (col("level") == "FATAL") & (col("message_content").contains("machine check interrupt")))

# Count the occurrences
result = filtered_entries.count()

print("Number of fatal log entries on Monday resulting from a 'machine check interrupt':", result)

spark.stop()
