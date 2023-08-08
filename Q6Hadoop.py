#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
#6. For each day of the week, what is the average number of seconds over which "re-synch state events" occurred?
# Define an MRJob class for calculating the average number of seconds over which "re-synch state events" occurred for each day of the week.
class MRJobAvgNoSec(MRJob):

    # Mapper function: Extracts relevant information from log lines and emits day of the week and seconds for "re-synch state events".
    def avg_seconds_day_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date = fields[2]
            day_of_week = datetime.datetime.strptime(date, "%Y.%m.%d").strftime("%A")
            message_content = ' '.join(fields[9:])
            if "re-synch state event" in message_content and "seconds" in fields[-1]:
                seconds = fields[-2]
                yield day_of_week, int(seconds)
                
    # Combiner function: Combines data within a single day to calculate the sum and count of seconds.
    def avg_seconds_day_combiner(self, day_of_week, seconds):
        total_seconds, count = 0, 0
        for sec in seconds:
            total_seconds += sec
            count += 1
        yield day_of_week, (total_seconds, count)

    # Reducer function: Calculates the average number of seconds for "re-synch state events" on each day of the week.
    def avg_seconds_day_reducer(self, day_of_week, total_count_pairs):
        total_seconds, total_count = 0, 0
        for sec, count in total_count_pairs:
            total_seconds += sec
            total_count += count
        avg_seconds = total_seconds / total_count
        yield day_of_week, avg_seconds


    def steps(self):
        return [
            MRStep(mapper=self.avg_seconds_day_mapper,
                   combiner=self.avg_seconds_day_combiner,
                   reducer=self.avg_seconds_day_reducer)
        ]
if __name__ == '__main__':
    MRJobAvgNoSec.run()
