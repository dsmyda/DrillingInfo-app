"""

@author: Danny Smyda
@date: 11-17-16

"""
import functions
import threading

class Progess():
    """ Used to keep track of the number of completed calls. __str__ method is invoked when the user types the command
    'active_jobs'."""

    def __init__(self):
        self.progress = 0.0

    def update(self, update):
        self.progress = update

    def get(self):
        return self.progress

    def __str__(self):
        if self.progress % 1 != 0:
            return "%{}".format(round(self.progress * 100,2))
        else:
            return "{}".format(self.progress)

class ApiThread (threading.Thread):
    """ Light-weight thread object that will call the retrieve function with its run body. This will allow for concurrency
    on different states. """

    def __init__(self, request_url, api_key, type, state, page_size, page_num):
        threading.Thread.__init__(self)
        self.request_url = request_url
        self.api_key = api_key
        self.type = type
        self.page_size = page_size
        self.page_num = page_num
        self.state = state
        self.completed = Progess()
        self.terminated = False

    def run(self):
        functions.retrieve(self, self.request_url, self.api_key, self.type, self.state, self.completed,
                                  self.page_size, self.page_num)

    def getName(self):
        return self.type + " for " + self.state

    def getProgress(self):
        return self.completed.__str__()

    def terminate(self):
        self.terminated = True

    def getTerminationFlag(self):
        return self.terminated

def distribute(request_url, api_key, type, states, pn, ps):
    """ Function that distributes the state requests to various threads.

    return: list of started thread objects, REPL in main will use it to check the status of the threads.
    """

    thread_objects = []

    if states:
        for state in states:
            thread_objects.append(ApiThread(request_url, api_key, type, state, ps, pn))

    for thread in thread_objects:
        thread.start()

    return thread_objects