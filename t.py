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
        self.count_all-=1
        top_five = []  # list to store top 5 key-count pairs
        for count in counts:
            if len(top_five) < 5:
                top_five.append((hour, count))
            else:
                min_count = min(top_five, key=lambda x: x[1])[1]
                if count > min_count:
                    min_index = top_five.index(min(top_five, key=lambda x: x[1]))
                    top_five[min_index] = (hour, count)
        top_five_sorted = sorted(top_five, key=lambda x: x[1], reverse=True)[:5]
        
        if self.count_all==0 :
            for pair in top_five_sorted:
                yield pair[0], pair[1]


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_hour,
                   combiner=self.combiner_sum_counts,
                   reducer=self.reducer_sum_counts),
            MRStep(reducer=self.reducer_find_top_five)
        ]
if __name__ == '__main__':
    MRTopFiveHoursJob.run()
