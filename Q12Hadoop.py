#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime


#12. What are the top 5 most frequently occurring hours in the log?

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
    
    def top_five(hour, counts):
        top_five = {} # dictionary to store top 5 key-count pairs
        for i in range(len(hour)):
            count = counts[i]
            if len(top_five) < 5:
                top_five[hour[i]] = count
            else:
                min_count = min(top_five.values())
                if count > min_count:
                    min_key = min(top_five, key=top_five.get)
                    del top_five[min_key]
                    top_five[hour[i]] = count
        return top_five

    def top_five1(self, hour, values):
        top_five = []
        for value in values:
            if len(top_five) > 5:
                if value > min(top_five):
                    top_five.remove(min(top_five))
                    top_five.append(value)
            else:
                top_five.append(value)
        
            

    def steps(self):
        return [
            MRStep(mapper=self.top_five_hours_mapper,
                   combiner=self.top_five_hours_combiner,
                   reducer=self.top_five_hours_reducer),
            MRStep(reducer=self.top_five)
        ]
if __name__ == '__main__':
    MRTopFiveHoursJob.run()
