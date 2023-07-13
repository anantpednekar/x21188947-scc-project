#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class MRTopFiveHoursJob(MRJob):
    
    count_all = 0
    def mapper_get_hour(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date_time = fields[4]
            hour = datetime.datetime.strptime(date_time, "%Y-%m-%d-%H.%M.%S.%f").hour
            yield hour, 1

    def combiner_sum_counts(self, hour, counts):
        yield hour, sum(list(counts))
    
    def reducer_sum_counts(self, hour, counts):
        self.count_all+=1
        yield hour, sum(list(counts))

    def reducer_find_top_five(self, hour, counts):
        yield 


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_hour,
                   combiner=self.combiner_sum_counts,
                   reducer=self.reducer_sum_counts),
            MRStep(reducer=self.reducer_find_top_five)
        ]
if __name__ == '__main__':
    MRTopFiveHoursJob.run()
