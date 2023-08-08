#! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

# This MRJob finds the number of fatal log entries that occurred on a Monday
# and resulted from a "machine check interrupt".

class MRFatalLogEntriesMonMcCkIntJob(MRJob):

    # The mapper takes a line of text from the log file and splits it into
    # fields. It then checks the day of the week, the log level, and the message
    # content to see if the entry is a fatal log entry from Monday that
    # resulted from a machine check interrupt. If it is, the mapper yields
    # the key None and the value 1.

    def fatallog_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date = fields[2]
            day_of_week = datetime.datetime.strptime(date, "%Y.%m.%d").strftime("%A")
            level = fields[8]
            message_content = ' '.join(fields[9:])
            if day_of_week == "Monday" and level == "FATAL" and "machine check interrupt" in message_content:
                yield None, 1

    # The combiner simply sums the values emitted by the mapper.

    def fatallog_combiner(self, _, values):
        yield None, sum(list(values))

    # The reducer takes the key and value pairs emitted by the combiner and
    # sums the values. It then yields the key and the sum as a result.

    def fatallog_reducer(self, _, counts):
        yield "Number of fatal log entries on Monday resulting from a 'machine check interrupt': ", sum(list(counts))

    def steps(self):
        return [
            MRStep(mapper=self.fatallog_mapper,
                   combiner=self.fatallog_combiner,
                   reducer=self.fatallog_reducer)
        ]

if __name__ == '__main__':
    MRFatalLogEntriesMonMcCkIntJob.run()
