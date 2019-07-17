import logging
from collections import deque

class Entry:
    def __init__(self, timestamp, level, message):
        self.timestamp = timestamp
        self.level = level
        self.message = message

class DequeLoggerHandler(logging.Handler):

    def __init__(self, max):
        logging.Handler.__init__(self)
        self.maxentries = max
        self.entries = deque(maxlen=self.maxentries)

    def emit(self, record):
        msg = self.format(record)
        timestamp = " ".join(msg.split(" ", 2)[:2])
        level = "".join(msg.split(" ", 3)[2:3])
        message = "".join(msg.split(" ", 3)[3:])
        self.entries.append(Entry(timestamp, level, message))

    def get_entries(self):
        copy = self.entries.copy()
        copy.reverse()
        return copy

