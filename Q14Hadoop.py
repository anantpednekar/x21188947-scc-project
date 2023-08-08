#! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

# This MRJob finds the node that generated the smallest number of KERNRTSP events.

class MRJobSmallestEvents(MRJob):

    # The mapper takes a line of text from the log file and splits it into
    # fields. It then checks for the "KERNRTSP" flag. If the flag is present,
    # the mapper yields the node and the number 1 as a key-value pair.

    def smallest_number_of_events_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            node = fields[3]
            flag = fields[0]
            if "KERNRTSP" in flag:
                yield node, 1 

    # The combiner simply sums the counts for each node.

    def smallest_number_of_events_combiner(self, node, count):
        yield node, sum(list(count))

    # The reducer takes the key-value pairs emitted by the combiner and
    # calculates the total number of events for each node. It then yields
    # the node and the total number of events as a result.

    def smallest_number_of_events_reducer(self, node, counts):
        total_count = sum(counts)
        yield None, (node, total_count)

    # The second reducer finds the nodes with the smallest number of events.

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
