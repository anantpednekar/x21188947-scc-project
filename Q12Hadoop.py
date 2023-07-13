#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class MRTopFiveHoursJob(MRJob):

    def mapper_get_hour(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date_time = fields[4]
            hour = datetime.datetime.strptime(date_time, "%Y-%m-%d-%H.%M.%S.%f").hour
            yield hour, 1

    def combiner_sum_counts(self, hour, count):  
       yield hour,sum(list(count))

    def reducer_sum_counts(self, hour, count):  
       yield None,(hour,sum(list(count)))
       
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
            yield None, (hour,count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_hour,
                   combiner=self.combiner_sum_counts,
                   reducer=self.reducer_sum_counts),
            MRStep(combiner=self.combiner_top_five_hours,
                   reducer=self.reducer_top_five_hours)
        ]

if __name__ == '__main__':
    MRTopFiveHoursJob.run()
