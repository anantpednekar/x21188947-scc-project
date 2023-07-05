#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class MRFatalLogEntriesMonMcCkIntJob(MRJob):
    def fatallog_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date = fields[2]
            day_of_week = datetime.datetime.strptime(date, "%Y.%m.%d").strftime("%A")
            level = fields[8]
            message_content = ' '.join(fields[9:])
            if day_of_week == "Monday" and level == "FATAL" and "machine check interrupt" in message_content:
                yield None, 1

    def fatallog_combiner(self, _, values):
            yield None, sum(list(values))

    def fatallog_reducer(self, _, counts):
        yield "Total ", sum(list(counts))
        #print("Total:", sum(list(counts)))  # Print the total

    def steps(self):
        return [
            MRStep(mapper=self.fatallog_mapper,
                   combiner=self.fatallog_combiner,
                   reducer=self.fatallog_reducer)
        ]
if __name__ == '__main__':
    MRFatalLogEntriesMonMcCkIntJob.run()
