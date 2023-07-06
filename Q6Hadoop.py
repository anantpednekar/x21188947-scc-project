#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class MRJobAvgNoSec(MRJob):
    def avg_seconds_day_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date = fields[2]
            day_of_week = datetime.datetime.strptime(date, "%Y.%m.%d").strftime("%A")
            message_content = ' '.join(fields[9:])
            if "re-synch state event" in message_content:
                yield day_of_week, 1

    def avg_seconds_day_reducer(self, day_of_week , counts):
        day = list(counts)
        yield day_of_week, sum(day)/len(day)

    def steps(self):
        return [
            MRStep(mapper=self.avg_seconds_day_mapper,
                   reducer=self.avg_seconds_day_reducer)
        ]
if __name__ == '__main__':
    MRJobAvgNoSec.run()
