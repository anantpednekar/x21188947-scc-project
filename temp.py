#! /usr/bin/env python

from pyspark.sql import SparkSession
import datetime

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .master("local[1]") \
    .appName("LogAnalysis") \
    .getOrCreate()

bgl_log_rdd = spark.sparkContext.textFile("BGLnew.csv")

# Define a function to check if the log entry is a fatal entry on a Monday with "machine check interrupt"
def is_fatal_monday_machine_check(row):
    fields = row.split(",")
    date = fields[2]
    day_of_week = datetime.datetime.strptime(date, "%Y.%m.%d").strftime("%A")
    level = fields[7]  # Assuming "FATAL" is in the 8th column, adjust if necessary
    message_content = fields[8]  # Assuming "machine check interrupt" is in the 9th column, adjust if necessary
    if day_of_week == "Monday" and level == "FATAL" and "machine check interrupt" in message_content :
        return True
    return False

# Load data from the CSV file and filter for fatal log entries on Monday with "machine check interrupt"
fatal_entries = bgl_log_rdd.filter(is_fatal_monday_machine_check)

# Count the number of fatal log entries that match the criteria
num_fatal_entries = fatal_entries.count()

print("Number of fatal log entries on a Monday resulting from a 'machine check interrupt':", num_fatal_entries)

#------------------------------from pyspark.sql import SparkSession


# Define a function to check if the log entry is a fatal entry on a Monday with "machine check interrupt"
def is_fatal_monday_machine_check(row):
    fields = row.split(",")
    date = fields[2]
    day_of_week = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%A")
    level = fields[7]  # Assuming "FATAL" is in the 8th column, adjust if necessary
    message_content = fields[8]  # Assuming "machine check interrupt" is in the 9th column, adjust if necessary
    if day_of_week == "Monday" and level == "FATAL" and "machine check interrupt" in message_content:
        return True
    return False

# Load data from the CSV file and filter for fatal log entries on Monday with "machine check interrupt"
fatal_entries = bgl_log_rdd.filter(is_fatal_monday_machine_check)

# Count the number of fatal log entries that match the criteria
num_fatal_entries = fatal_entries.count()

print("Number of fatal log entries on a Monday resulting from a 'machine check interrupt':", num_fatal_entries)
