from master import ThreadStorage
from unittest import TestCase, main
import threading

class Test_T(threading.Thread):
    def __init__(self, key):
        super().__init__()
        self.k = key

    def __lt__(self, other):
        return self.k < other.k

class TestStorage(TestCase):
    pass

if __name__ == '__main__':
    # Run all of the tests, print the results, and exit.
    main(verbosity=2)