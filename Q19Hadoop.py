#! /usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

#19. On which date was the latest fatal app error where the message contains "Error reading message prefix"?

class MRJobLatestFatalError(MRJob):
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

    def latest_fatal_error_combiner(self, _, timestamps):
        latest_timestamp = None
        for timestamp_str in timestamps:
            date = datetime.datetime.fromtimestamp(int(timestamp_str)).date()
            datetime_obj = datetime.datetime.combine(date, datetime.time.min)
            timestamp = int(datetime_obj.timestamp())
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
        yield None, latest_timestamp

    def latest_fatal_error_reducer(self, _, timestamps):
        latest_timestamp = None
        for timestamp_str in timestamps:
            date = datetime.datetime.fromtimestamp(int(timestamp_str)).date()
            datetime_obj = datetime.datetime.combine(date, datetime.time.min)
            timestamp = int(datetime_obj.timestamp())
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
        if latest_timestamp:
            date_str = latest_timestamp.strftime("%Y.%m.%d")
            yield "latest fatal with Error reading message prefix", date_str

    def steps(self):
        return [
            MRStep(mapper=self.latest_fatal_error_mapper,
                   combiner=self.latest_fatal_error_combiner,
                   reducer=self.latest_fatal_error_reducer)
        ]

if __name__ == '__main__':
    MRJobLatestFatalError.run()