"""
@author: Danny Smyda
"""

from abc import ABCMeta, abstractmethod
import json

class Buffer:

    _BUFFER_SIZE = 25

    def __init__(self):
        self.fresh = True
        self.buffer = []

    __metaclass__ = ABCMeta

    @abstractmethod
    def add(self, item): pass

    @abstractmethod
    def flush(self): pass

    @abstractmethod
    def close(self): pass

    def force_add(self, item): pass

    def is_empty(self):
        return len(self.buffer) == 0

class JSONBuffer(Buffer):

    _DELIMITER = ','

    def __init__(self, filepath):
        super().__init__()
        self.file = None
        self.filepath = filepath+".json"

    def add(self, obj):
        if len(self.buffer) > self._BUFFER_SIZE: self.flush()

        if not self.fresh:
            self.buffer.append(self._DELIMITER)
        else:
            self.fresh = False

        self.buffer.append(json.dumps(obj))

    def flush(self):
        if not self.file: self.file = open(self.filepath, 'w')
        for item in self.buffer:
            self.file.write(item)
        self.buffer.clear()

    def close(self):
        self.flush()
        self.file.close()

    def force_add(self, obj):
        self.buffer.append(obj)

class CSVBuffer(Buffer):

    def __init__(self, filepath):
        super().__init__()
        self.file = None
        self.writer = None
        self.filepath = filepath+".csv"

    def add(self, obj):
        if len(self.buffer) > self._BUFFER_SIZE: self.flush()
        self.buffer.append(obj)

    def flush(self):
        if not self.file: self.file = open(self.filepath, 'w')

        if not self.writer and not self.is_empty():
            from csv import DictWriter
            self.writer = DictWriter(self.file, self.buffer[0].keys())
            self.writer.writeheader()

        for item in self.buffer:
            self.writer.writerow(item)
        self.buffer.clear()

    def close(self):
        self.flush()
        self.file.close()

    def force_add(self, obj):
        self.buffer.append(obj)