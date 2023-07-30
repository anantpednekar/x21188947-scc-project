#! /usr/bin/env python
import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .master("local[1]") \
    .appName("bgl_log_app") \
    .getOrCreate()

def fatallog_mapper(line):
    fields = line.split()
    if len(fields) >= 8:
        date = fields[2]
        day_of_week = datetime.datetime.strptime(date, "%Y.%m.%d").strftime("%A")
        level = fields[8]
        message_content = ' '.join(fields[9:])
        if day_of_week == "Monday" and level == "FATAL" and "machine check interrupt" in message_content:
            return 1
    return 0

results = spark.sparkContext.textFile("BGL.log") \
    .map(fatallog_mapper) \
    .reduce(lambda x, y: x + y)

print("Number of fatal log entries on Monday resulting from a 'machine check interrupt':", results)
