import os
import shutil
from master import Master

#src/utilities/AtomicFileHandler.py
#Written by Danny Smyda

class AtomicFileHandler:
    def __init__(self, path):
        self._path = path
        #Create if does not exist...
        #TODO FIX IF PERMISSION DENIED
        open(self._path, 'a').close()
        self.__master_ptr = Master()
        self._shadow_copy()
    def _rand_path(self):
        return os.getcwd() + os.sep + "tmp" + os.sep + os.path.basename(self._path)
    def _shadow_copy(self):
        self._t_path = self._rand_path()
        self._t_fd = open(self._t_path, 'wb')
        with open(self._path, 'rb') as fsrc:
            shutil.copyfileobj(fsrc, self._t_fd, 10 * 1024)
        self._t_fd.close()
        self._t_fd = open(self._t_path, 'a')
        self.write = self._t_fd.write
    def checkpoint_commit(self, status, d):
        if not self._t_fd.closed:
            self._t_fd.close()
            self.atomic_rename(status, d)
            self._shadow_copy()                          #BLOCKS UNTIL FULL TRANSFER
    def close(self, status, d):
        if not self._t_fd.closed:
            self._t_fd.close()
            self.atomic_rename(status, d)
    def atomic_rename(self, status, d):
        self.__master_ptr.log("%s", 'R', extra=d)      #Append atomicity
        os.rename(self._t_path, self._path)  # ATOMIC BABY!!!
        d['descriptor'] = self._path
        self.__master_ptr.log("%s", status, extra=d) #Append atomicity