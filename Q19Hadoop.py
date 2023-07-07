#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

#19. On which date was the latest fatal app error where the message contains "Error reading message prefix"?
class MRJobLatestFatalError(MRJob):
    def latest_fatal_error_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date = fields[2]
            level = fields[8]
            message_content = ' '.join(fields[9:])
            if level == "FATAL" and "Error reading message prefix" in message_content:
                yield date, None


    def steps(self):
        return [
            MRStep(mapper=self.latest_fatal_error_mapper,
                   combiner=self.latest_fatal_error_combiner,
                   reducer=self.latest_fatal_error__reducer)
        ]
if __name__ == '__main__':
    MRJobLatestFatalError.run()
