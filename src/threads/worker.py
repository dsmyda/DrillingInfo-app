"""

@author: Danny Smyda
@date: 11-17-16

"""
from write_buffers import JSONBuffer, CSVBuffer

import threading
import json
import os
import master

#Decorator for the threads run method
def process(run):
    def run_wrapper(self):
        try:
            if self.format == "JSON":
                self.buffer.force_add("[")
            run(self)
        except LookupError as e:
            master.log.exception("message")
        finally:
            if self.format == "JSON":
                self.buffer.force_add("]")
            self.buffer.close()
            master.remove_active(self)

    return run_wrapper

class Worker(threading.Thread):

    """ An basic worker thread.

    Subclasses are responsible for maintaining progress information
    and naming features. """

    def __init__(self, file_path, format):
        threading.Thread.__init__(self)

        self.format = format
        self.suspended = False
        self.pause_condition = threading.Condition(threading.Lock())

        self.cnt = 0.0

        if self.is_csv():
            self.buffer = CSVBuffer(file_path)
        else:
            self.buffer = JSONBuffer(file_path)

    @process
    def run(self):
        pass

    def commit_results(self, response):
        for obj in response:
            self.buffer.add(obj)
            self.completed()

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

    def is_json(self):
        return self.format == "JSON"

    def is_csv(self):
        return self.format == "CSV"