#! /usr/bin/env python

# Import the necessary modules.
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

# This is the main class for the MapReduce job.
class MRTopFiveHoursJob(MRJob):

    # The mapper function takes a line from the input log file as input and
    # emits the hour of the day and a count of 1 for each line.
    def mapper_get_hour(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            # Get the hour from the date-time field in the log line.
            date_time = fields[4]
            hour = datetime.datetime.strptime(date_time, "%Y-%m-%d-%H.%M.%S.%f").hour
            # Emit the hour and a count of 1.
            yield hour, 1

    # The combiner function sums the counts for each hour.
    def combiner_sum_counts(self, hour, count):
        # Yield the hour and the sum of the counts.
        yield hour,sum(list(count))

    def reducer_sum_counts(self, hour, count):  
       yield None,(hour,sum(list(count)))


    # The combiner function for the top 5 hours step sums the counts for each
    # hour.
    def combiner_top_five_hours(self, _, hours_counts):
        topn = dict()
        for hour_count in hours_counts:
            hour=hour_count[0]
            count=hour_count[1]
            if len(topn)==5:
                current_lowest = min(topn.values())
                if current_lowest < count:
                    idx = list(topn.values()).index(current_lowest)
                    key =  list(topn.keys())[idx]
                    del topn[key]
                    topn[hour] = count
            else:
                topn[hour] = count
        for hour,count in topn.items():
            yield None, (hour,count)


    # The reducer function for the top 5 hours step sorts the hours and counts
    # in descending order and yields the top 5 most frequently occurring hours.
    def reducer_top_five_hours(self, _, hours_counts):
        topn = dict()
        for hour_count in hours_counts:
            hour=hour_count[0]
            count=hour_count[1]
            if len(topn)==5:
                current_lowest = min(topn.values())
                if current_lowest < count:
                    idx = list(topn.values()).index(current_lowest)
                    key =  list(topn.keys())[idx]
                    del topn[key]
                    topn[hour] = count
            else:
                topn[hour] = count
        for hour,count in sorted(topn.items(), key=lambda x: x[1], reverse=True):
            yield " The Top 5 most frequently occurring hours [Hours , Count] ", (hour,count)

    # This method defines the steps for the MapReduce job.
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_hour,
                   combiner=self.combiner_sum_counts,
                   reducer=self.reducer_sum_counts),
            MRStep(combiner=self.combiner_top_five_hours,
                   reducer=self.reducer_top_five_hours)
        ]

# This is the main entry point for the MapReduce job.
if __name__ == '__main__':
    MRTopFiveHoursJob.run()
