#! /usr/bin/env python
import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .config("spark.driver.host", "localhost") \
      .master("local[1]") \
      .appName("bgl_log_app") \
      .getOrCreate() 

# define a function to parse the log line and return the timestamp and the message content
def parse_log_line(line):
    fields = line.split()
    if len(fields) >= 8:
        date_str = fields[2]
        level = fields[8]
        message_content = ' '.join(fields[9:])
        if level == "FATAL" and "Error reading message prefix" in message_content:
            date = datetime.datetime.strptime(date_str, "%Y.%m.%d")
            timestamp = int(date.timestamp())
            return timestamp

# read the log file into an RDD
log_rdd = spark.sparkContext.textFile("BGL.log")

# parse the log file and filter the data to return only fatal errors with the message "Error reading message prefix"
filtered_rdd = log_rdd.map(parse_log_line).filter(lambda x: x is not None)

# get the latest timestamp using the max function
latest_timestamp = filtered_rdd.max()

# define a function to get the date of the latest timestamp
def get_date_of_latest_timestamp(timestamp):
    date = datetime.datetime.fromtimestamp(int(timestamp)).date()
    return str(date)

# get the date of the latest timestamp
date = get_date_of_latest_timestamp(latest_timestamp)

# print the date of the latest timestamp
print("The latest fatal app error with the specified message occurred on:", date)
