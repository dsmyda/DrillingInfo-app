import requests, threading, os
from abc import abstractmethod
from csv import DictWriter
from PyQt5 import QtCore
from AtomicFileHandler import AtomicFileHandler
from master import Master

#/src/api/t_http.py

def run_decorator(run):
    def run_wrapper(self):
        try:
            run(self)
        finally:
            self.update_d(self.get_id(), self.get_sp(), self.get_ps())
            self.close_fileHandler()
            self.remove_self()
            self._trigger.emit('%s/%s/%s' % (self.__repr__(), self.progress(), self.get_key()))
            self.quit()
    return run_wrapper

class HTTP_T(QtCore.QThread):
    _trigger = QtCore.pyqtSignal(object)
    _SIZE = 2000
    def __init__(self, file_path, key):
        QtCore.QThread.__init__(self)
        self.__suspended = False
        self.__pause_condition = threading.Condition(threading.Lock())
        self.__status = 'ic'
        self.__running = True
        self.__d = {}
        self.__writer = None
        self.__fresh = True
        self.__key = key
        self.__cnt = 0.0
        self.atom = AtomicFileHandler(file_path)
        self.master = Master()
    @run_decorator
    def run(self):
        pass
    def finished(self):
        self.__status = 'c'
    def safe_kill(self):
        self.__running = False
    def update_d(self, id, sp, ps):
        self.__d['pagenum'] = sp
        self.__d['pagesize'] = ps
        self.__d['id'] = id
    def write(self, response, id, sp, ps):
        if not self.__writer:
            self.__writer = DictWriter(self.atom, response[0].keys())
        if self.__fresh:
            self.__writer.writeheader()
            self.__fresh = False
        for obj in response:
            self.__writer.writerow(obj)
        if self.__cnt > 0 and self.__cnt % self._SIZE == 0:
            self.update_d(id, sp, ps)
            self.atom.checkpoint_commit(self.__status, self.__d)
            self.__writer = None
    def completed(self):
        self.__cnt += 1.0
        self._trigger.emit('%s/%s/%s' % (self.__repr__(), self.progress(), self.get_key()))
    def suspend_here(self):
        with self.__pause_condition:
            while self.__suspended:
                self.__pause_condition.wait()
    def suspend(self):
        self.__suspended = True
        self.__pause_condition.acquire()
        self._trigger.emit('%s/%s/%s' % (self.__repr__(), self.progress(), self.get_key()))
    def resume(self):
        self.__suspended = False
        self.__pause_condition.notify()
        self.__pause_condition.release()
        self._trigger.emit('%s/%s/%s' % (self.__repr__(), self.progress(), self.get_key()))
    def is_suspended(self):
        return self.__suspended and self.isRunning()
    def path_delimiter(self):
        return os.sep
    def retreive(self, url):
        return requests.get(url, headers={'X-API-KEY': self.api_key}, timeout=60)
    def set_descriptor(self, descriptor):
        self.__d['descriptor'] = descriptor
    def is_running(self):
        return self.__running
    def close_fileHandler(self):
        self.atom.close(self.__status, self.__d)
    def remove_self(self):
        self.master.remove(id(self))
    def get_cnt(self):
        return self.__cnt
    def set_cnt(self, cnt):
        self.__cnt = cnt
    def get_key(self):
        return self.__key
    def get_id(self): pass
    def get_sp(self): pass
    def get_ps(self): pass
    @abstractmethod
    def progress(self): pass
    def __repr__(self): pass
    def __lt__(self, other):
        return self.get_key() < other.get_key()