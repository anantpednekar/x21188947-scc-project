import datetime

timestamp_str = "1136246400"
date = datetime.datetime.fromtimestamp(int(timestamp_str)).date()

datetime_obj = datetime.datetime.combine(date, datetime.time.min)
timestamp = int(datetime_obj.timestamp())

print(date)

print(timestamp)