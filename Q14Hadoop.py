#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

#14. Which node generated the smallest number of KERNRTSP events?

class MRJobSmallestEvents(MRJob):
    def smallest_number_of_events_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            node = fields[3]
            flag = fields[0]
            if "KERNRTSP" in flag:
                yield node, 1 

    def smallest_number_of_events_combiner(self, node, count):
        yield node, sum(list(count))

    def smallest_number_of_events_reducer(self, node, count):
        yield node, sum(list(count))
    
    def final_reducer(self, node, count):
        #not working Min
        yield node, min(list(count))
        
    def steps(self):
        return [
            MRStep(mapper=self.smallest_number_of_events_mapper,
                   combiner=self.smallest_number_of_events_combiner,
                   reducer=self.smallest_number_of_events_reducer),
            MRStep(reducer=self.final_reducer),
        ]
if __name__ == '__main__':
    MRJobSmallestEvents.run()
