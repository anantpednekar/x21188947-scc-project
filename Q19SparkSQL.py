#! /usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType
from pyspark.sql.functions import col

# Create a Spark session
session = SparkSession.builder.appName("LatestPrefixError").getOrCreate()

# Define the schema for the DataFrame
bgl_log_schema = StructType([
    StructField("AlertFlag", StringType()),
    StructField("Timestamp", LongType()),
    StructField("Date", DateType()),
    StructField("Node", StringType()),
    StructField("DateTime", StringType()),
    StructField("NodeRepeated", StringType()),
    StructField("MessageType", StringType()),
    StructField("SystemComponent", StringType()),
    StructField("Level", StringType()),
    StructField("MessageContent", StringType())
])

# Read the CSV file and create a DataFrame
bgl_log_df = session.read.csv(
    "BGLnew.csv",
    schema=bgl_log_schema,
    header=False,
    dateFormat="yyyy.MM.dd"
)

# Create a temporary view for the DataFrame to use SQL queries
bgl_log_df.createOrReplaceTempView("bgl_log")


# Select the maximum Date from the filtered data using a subquery
# Filter the DataFrame to include only rows where the Level is FATAL
# and the MessageContent contains "Error reading message prefix"
query = """
    SELECT MAX(Date) AS LatestDate
    FROM (  
        SELECT *
        FROM bgl_log
        WHERE Level = 'FATAL' AND INSTR(MessageContent, 'Error reading message prefix') > 0
    ) AS filtered_data
"""

# Execute the query
result_df = session.sql(query)

# Get the latest date value from the DataFrame
latest_date = result_df.first()["LatestDate"]

# Print the latest date
print("The latest fatal app error with the specified message occurred on:", latest_date)

