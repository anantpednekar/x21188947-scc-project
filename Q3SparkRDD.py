#! /usr/bin/env python

import datetime
from pyspark.sql import SparkSession

# This script uses PySpark to find the number of fatal log entries that occurred
# on a Monday and resulted from a "machine check interrupt".

# Create a SparkSession.
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .master("local[1]") \
    .appName("bgl_log_app") \
    .getOrCreate()

# Define a function to filter the log entries.
def fatallog_filter(line):
    """
    Filters the log entries to only include those that are fatal and occurred on
    a Monday and resulted from a machine check interrupt.

    Args:
        line (str): A line of text from the log file.

    Returns:
        int: 1 if the line is a match, 0 otherwise.
    """
    fields = line.split()
    if len(fields) >= 8:
        date = fields[2]
        day_of_week = datetime.datetime.strptime(date, "%Y.%m.%d").strftime("%A")
        level = fields[8]
        message_content = ' '.join(fields[9:])
        if day_of_week == "Monday" and level == "FATAL" and "machine check interrupt" in message_content:
            return 1
    return 0

# Get the results of the filter.
results = spark.sparkContext.textFile("BGL.log") \
    .map(fatallog_filter) \
    .reduce(lambda x, y: x + y)

# Print the results.
print("Number of fatal log entries on Monday resulting from a 'machine check interrupt':", results)
