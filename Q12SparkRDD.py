#! /usr/bin/env python
from pyspark import SparkConf, SparkContext
from operator import add
import datetime

# Create SparkConf and SparkContext
conf = SparkConf().setAppName("TopFiveHours")
sc = SparkContext(conf=conf)

# Read the log file into an RDD
log_lines_rdd = sc.textFile("BGLnew.csv")

# Extract the hour from the log line
def extract_hour(line):
    date_time = line.split()[4]
    hour = datetime.datetime.strptime(date_time, "%Y-%m-%d-%H.%M.%S.%f").hour
    return hour

# Map the RDD to extract hours and then reduce by key to count occurrences
hour_counts_rdd = log_lines_rdd.map(extract_hour).map(lambda hour: (hour, 1)).reduceByKey(add)

# Get the top 5 most frequently occurring hours
top_five_hours = hour_counts_rdd.takeOrdered(5, key=lambda x: -x[1])

# Print the top 5 most frequently occurring hours
for hour, count in top_five_hours:
    print(f"Hour: {hour}, Count: {count}")

# Stop the SparkContext
sc.stop()
