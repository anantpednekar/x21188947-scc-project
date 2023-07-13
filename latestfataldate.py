#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MRJobLatestFatalError(MRJob):
    latest_date = None

    def latest_fatal_error_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date_str = fields[2]
            level = fields[8]
            message_content = ' '.join(fields[9:])
            if level == "FATAL" and "Error reading message prefix" in message_content:
                yield 1 ,date_str
                
    def latest_fatal_error_reducer(self,count,date):
        #date = datetime.strptime(date, "%Y.%m.%d")
        yield sum(list(count)),date
        #if self.latest_date is None or date > self.latest_date:
           # self.latest_date = date

    def reducer_final(self):
        if MRJobLatestFatalError.latest_date:
             yield "Latest Date on which FATAL Error reading message prefix occured", MRJobLatestFatalError.latest_date

    def steps(self):
        return [
            MRStep(mapper=self.latest_fatal_error_mapper,
                   reducer=self.latest_fatal_error_reducer)
        ]

if __name__ == '__main__':
    MRJobLatestFatalError.run()
