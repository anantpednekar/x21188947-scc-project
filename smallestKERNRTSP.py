#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

#14. Which node generated the smallest number of KERNRTSP events?

class MRJobSmallestEvents(MRJob):
    def mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            node = fields[3]
            flag = fields[0]
            if flag == "KERNRTSP":
                yield node, 1

    def reducer(self, node, counts):
        yield None, (sum(counts), node)

    def final_reducer(self, _, values):
        min_count = float('inf')
        min_node = None
        for count, node in values:
            if count < min_count:
                min_count = count
                min_node = node
        yield min_node, min_count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]

if __name__ == '__main__':
    MRJobSmallestEvents.run()


