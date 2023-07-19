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

    def smallest_number_of_events_reducer(self,node, counts):
        total_count = sum(counts)
        yield None, (node, total_count)

    def reducer_smallest_nodes(self, _, node_counts):
            min_count = None
            smallest_nodes = []
            for node_count in node_counts:
                node = node_count[0]
                count = node_count[1]
                if min_count is None or count < min_count:
                    min_count = count
                    smallest_nodes = [(node, count)]
                elif count == min_count:
                    smallest_nodes.append((node, count))
            for node, count in smallest_nodes:
                yield " Nodes : ", (node, count)

    def steps(self):
        return [
            MRStep(mapper=self.smallest_number_of_events_mapper,
                   combiner=self.smallest_number_of_events_combiner,
                   reducer=self.smallest_number_of_events_reducer),
            MRStep(reducer=self.reducer_smallest_nodes)
        ]
if __name__ == '__main__':
    MRJobSmallestEvents.run()