#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

#14. Which node generated the smallest number of KERNRTSP events?

class MRTopFiveHoursJob(MRJob):
    def top_five_hours_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date_time = fields[4]
            hour = datetime.datetime.strptime(date_time, "%Y-%m-%d-%H.%M.%S.%f").hour
            yield hour, 1

    def top_five_hours_combiner(self, hour, values):
        yield hour, sum(list(values))

    def top_five_hours_reducer(self, hour, counts):
        
        yield hour, sum(list(counts))

    def steps(self):
        return [
            MRStep(mapper=self.fatallog_mapper,
                   combiner=self.fatallog_combiner,
                   reducer=self.fatallog_reducer)
        ]
if __name__ == '__main__':
    MRTopFiveHoursJob.run()
