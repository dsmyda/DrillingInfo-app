"""
@author: Danny Smyda
"""

class FileBuffer:

    """ A FileBuffer class to manage writing json files to disk.

    The buffer is responsible for formatting data, by """

    _BUFFER_SIZE = 25
    _DELIMITER = ','

    def __init__(self, filepath):
        self.fresh = True
        self.buffer = []
        self.file = None
        self.filepath = filepath

    def add(self, str):
        if not self.file: self.file = open(self.filepath, 'w')
        if len(self.buffer) > FileBuffer._BUFFER_SIZE: self.flush()

        if not self.fresh:
            self.buffer.append(FileBuffer._DELIMITER)
        else:
            self.fresh = False

        self.buffer.append(str)

    def flush(self):
        if not self.file: self.file = open(self.filepath, 'w')
        for item in self.buffer:
            self.file.write(item)
        self.buffer.clear()

    def close(self):
        self.flush()
        self.file.close()

    def force_add(self, str):
        self.buffer.append(str)
