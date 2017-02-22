"""

@author: Danny Smyda
@date: 11-17-16

"""
from write_buffer import FileBuffer

import threading
import json
import os
import csv
import master

#DECORATOR
def process(run):
    def run_wrapper(self):
        try:
            self.buffer.force_add("[")
            run(self)
        except LookupError as e:
            master.log.exception("message")
        finally:
            self.buffer.force_add("]")
            self.buffer.close()
            master.remove_active(self)

    return run_wrapper

class Worker(threading.Thread):

    """ An basic worker thread.

    Subclasses are responsible for maintaining progress information
    and naming features. """

    def __init__(self, file_path):
        threading.Thread.__init__(self)

        self.suspended = False
        self.pause_condition = threading.Condition(threading.Lock())

        self.cnt = 0.0
        self.buffer = FileBuffer(file_path)

    @process
    def run(self):
        pass

    def commit_results(self, response, format):
        if format == "json":
            for obj in response:
                self.buffer.add(json.dumps(obj))
                self.completed()
        elif format == "csv":
            for obj in response:
                self.buffer.add()

    def completed(self):
        self.cnt += 1.0

    def suspend_here(self):
        with self.pause_condition:
            while self.suspended:
                self.pause_condition.wait()

    def suspend(self):
        self.suspended = True
        self.pause_condition.acquire()

    def resume(self):
        self.suspended = False
        self.pause_condition.notify()
        self.pause_condition.release()

    def is_suspended(self):
        return self.suspended and self.is_alive()

    def progess(self):
        return "Unknown Progress"

    def __repr__(self):
        return "Unknown Worker"

    def path_delimiter(self):
        return os.sep

    def file_ext(self):
        return ".json"