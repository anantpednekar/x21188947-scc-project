#! /usr/bin/env python
# Import necessary modules
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

#19. On which date was the latest fatal app error where the message contains "Error reading message prefix"?
# Define an MRJob class to find the latest fatal app error with a specific message content.


class MRJobLatestFatalError(MRJob):
    
    # Mapper function: Extracts relevant information from log lines and emits timestamps for fatal errors with specified message content.
    def latest_fatal_error_mapper(self, _, line):
        fields = line.split()
        if len(fields) >= 8:
            date_str = fields[2]
            level = fields[8]
            message_content = ' '.join(fields[9:])
            if level == "FATAL" and "Error reading message prefix" in message_content:
                date = datetime.datetime.strptime(date_str, "%Y.%m.%d")
                timestamp = int(date.timestamp())
                yield None, timestamp
                
    # Combiner function: Identifies the latest timestamp among the emitted timestamps.
    def latest_fatal_error_combiner(self, _, timestamps):
        latest_timestamp = None
        for timestamp_str in timestamps:
            date = datetime.datetime.fromtimestamp(int(timestamp_str)).date()
            datetime_obj = datetime.datetime.combine(date, datetime.time.min)
            timestamp = int(datetime_obj.timestamp())
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
        yield None, latest_timestamp

    # Reducer function: Determines the latest fatal error timestamp and yields the corresponding date.
    def latest_fatal_error_reducer(self, _, timestamps):
        latest_timestamp = None
        for timestamp_str in timestamps:
            date = datetime.datetime.fromtimestamp(int(timestamp_str)).date()
            datetime_obj = datetime.datetime.combine(date, datetime.time.min)
            timestamp = int(datetime_obj.timestamp())
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
        if latest_timestamp:
            yield "latest fatal with Error reading message prefix", str(datetime.datetime.fromtimestamp(int(latest_timestamp)).date())

    def steps(self):
        return [
            MRStep(mapper=self.latest_fatal_error_mapper,
                   combiner=self.latest_fatal_error_combiner,
                   reducer=self.latest_fatal_error_reducer)
        ]

if __name__ == '__main__':
    MRJobLatestFatalError.run()