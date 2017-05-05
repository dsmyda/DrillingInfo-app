import heapq
import logging
import os
import threading
from time import strftime

from PyQt5 import QtCore

from states import abbrev

# src/master.py
# Written by Danny Smyda

#+======================================================================+#
#                                                                        #
# Super vanilla thread-safe queue - wanted to improve time bounds on     #
# just a simple list, but also add some functionality I can't get with   #
#                         the built in Queue.                            #
#                                                                        #
#   put() - adds the thread to the heap.                      O(log(n))  #
#   getHead() - removes the thread at the top of the heap.    O(log(n))  #
#   peak() - returns the top of the heap, without deleting         O(1)  #
#   remove() - removes the item and restores the invariant.        O(n)  #
#   union() - merges two heaps into one valid heap.         O(k*log(n))  #
#   decrease_key() - decreases any given thread in the heap.       O(n)  #
#   index() - finds the index of the thread in the heap.           O(n)  #
#   __contains__() - Determines if the thread is in the heap.      O(n)  #
#   __len__(): Returns the size of the heap.                       O(1)  #
#                                                                        #
#+======================================================================+#
class ThreadStorage:
    def __init__(self):
        #Used to store threads -- make sure it thread-safe.
        self.__lock = threading.Lock()
        self.__heap = []
    def put(self, t):
        #O(log(n)) for _siftup()
        with self.__lock:
            heapq.heappush(self.__heap, t)
    def getHead(self):
        #O(log(n)) for _siftdown()
        with self.__lock:
            return heapq.heappop(self.__heap)
    def peak(self):
        #O(1) - simply array idx
        with self.__lock:
            return self.__heap[0]
    def remove(self, t):
        #O(n) -- operation is dominated by the call to self.index().
        #_siftdown runs in O(log(n))
        with self.__lock:
            idx = self.index(t)
            self.__heap[idx] = self.__heap[-1]
            self.__heap.pop()
            if idx != len(self.__heap):         #minus 1, best case we popped the original idx off the back
                heapq._siftdown(self.__heap, 0, idx)
    def union(self, t_s):
        #O(k*log(n)) - k: len of t_s list, log(n): heapify worst case
        res = ThreadStorage()
        res.__heap = self.__heap
        with t_s.__lock:
            for t in t_s.__heap:
                heapq.heappush(res.__heap, t)   #No need to use thread-safe put, res is only known to this stack frame
        return res
    def decrease_key(self, t_, k):
        #O(n) operation, due to linear search
        if k > t_.key:
            KeyError("New key is larger than current key")
        idx = self.__heap.index(t_)     #O(n)
        t_.key = k
        heapq._siftup(self.__heap, idx)     #O(log(n))
    def index(self, t_):
        #O(n) for worst case, but practical optimization on key comparisons
        for idx, t in enumerate(self.__heap):
            if t == t_:
                return idx
            elif t.get_key() > t_.get_key():
                raise ValueError("%s is not present in heap" % t_)
        raise ValueError("%s is not present in heap" % t_)
    def __contains__(self, t_):
        #O(n) worse case, the index operation is clearly bottleneck
        try:
            self.index(t_)
            return True
        except ValueError:
            return False
    def __len__(self):
        #O(1)
        return len(self.__heap)
    def __iter__(self):
        return self.__heap.__iter__()

#+=============================================================================+#
#                                                                               #
#   Master class is responsible for thread management and access.               #
#   Important mediator between the user (issuing commands via the GUI)          #
#   and the underlying processes.                                               #
#                                                                               #
#   get() - Retrieve a thread from the thread map given its id.           O(1)  #
#   send() - Accept a thread, and either start it or queue it.       O(log(n))  #
#   active_cnt() - Return the number of active threads.                   O(1)  #
#   remove() - Remove a thread from either thread storage.                O(n)  #
#   pause() - Pause a given thread from its thread id.                    O(1)  #
#   resume() - Resume a given thread from its id.                         O(1)  #
#   queue() - Queue an active thread.                                     O(n)  #
#   status() - Retrieve the status of a given thread.                     O(n)  #
#   get_progress() - Gets the progress (%) of a thread.         Amortized O(1)  #
#   shutdown() - Safely shuts down a given thread.                        O(1)  #
#   exit() - Safely shuts down all given threads.                  O(k*log(n))  #
#   log_parse() - Parses the log file backwards.                          O(f)  #
#   recovery() - Parses log to recover previous state.          avg: O(log(f))  #
#   init_last_save() - Reload previous thread state.                      O(f)  #
#   has_crashed() - Determines if the app previously crashed.        avg: O(1)  #
#   get_save_id() - Get last saved entity id.                             O(1)  #
#   get_save_page() - Get last saved page number.                         O(1)  #
#   get_save_size() - Get last saved page size.                           O(1)  #
#                                                                               #
#+=============================================================================+#
class Master:
    __instance = None
    def __init__(self, max = 2):
        if max < 1: raise ValueError("Must specify a max >= 1.")
        if not Master.__instance:
            Master.__instance = Master.__SingletonMaster(max)
    def __getattr__(self, attr):
        return getattr(self.__instance, attr)
    def __setattr__(self, key, value):
        return setattr(self.__instance, key, value)

    class __SingletonMaster:
        class __SaveState:
            def __init__(self, e_id, pn, ps):
                self.__e_id = e_id
                self.__pn = pn
                self.__ps = ps
            def id(self): return self.__e_id
            def pn(self): return self.__pn
            def ps(self): return self.__ps

        def __init__(self, max = 2, ):
            self.__trigger = QtCore.pyqtSignal(object)
            self.__MAX_RUNNING = max
            self.__q = ThreadStorage()
            self.__active = ThreadStorage()
            self.__t_map = {}
            self.__lock = threading.Lock()
            hdlr = logging.FileHandler(strftime(os.getcwd()+ os.sep+"log"+os.sep+"activity.log"))
            hdlr.setFormatter(logging.Formatter("%(message)s %(descriptor)s %(id)s %(pagenum)s %(pagesize)s"))
            self.__log = logging.getLogger("activity.log")
            self.__log.addHandler(hdlr)
        def increase_priority(self, t_id):
            t = self.get(t_id)
            if t in self.__q:
                self.__q.decrease_key(t, t.key - 1)
        def log(self, msg, *args, **kwargs):
            self.__log.warning(msg, *args, **kwargs)
        def get(self, t_id):
            return self.__t_map[t_id]
        def get_key(self, t_id):
            return self.get(t_id).get_key()
        def send(self, t):
            with self.__lock:
                self.__t_map[id(t)] = t
                if(self.active_cnt() < self.__MAX_RUNNING):
                    t.start()
                    self.__active.put(t)
                else:
                    self.__q.put(t)
        def active_cnt(self):
            return len(self.__active)
        def total_cnt(self):
            return len(self.__t_map)
        def remove(self, t_id):
            with self.__lock:
                t = self.get(t_id)
                if t in self.__active:
                    self.__active.remove(t)
                    self.__start_queued()
                elif t in self.__q:
                    self.__q.remove(t)
                #self.__trigger("%")
                self.__t_map.pop(t_id)
        def __start_queued(self):
            if len(self.__q) > 0:
                t = self.__q.getHead()
                if t.is_suspended():
                    t.resume()
                else:
                    t.start()
                self.__active.put(t)
        def pause(self, t_id):
            t = self.get(t_id)
            if not t.is_suspended():
                t.suspend()
        def resume(self, t_id):
            t = self.get(t_id)
            if t.is_suspended():
                t.resume()
        def queue(self, t_id):
            t = self.get(t_id)
            if t in self.__q: return
            if not t.is_suspended():
                t.suspend()
            if len(self.__q) > 1:
                self.__q.put(t)
                self.__active.remove(t)
        def status(self, t_id):
            # 1 = active, 2 = queued, 3 = paused, -1 not found
            try:
                t = self.get(t_id)
                if t in self.__active and t.is_suspended():
                    return 3
                elif t in self.__q:
                    return 2
                elif t in self.__active:
                    return 1
            except KeyError:
                return -1
        def get_progress(self, t_id):
            t = self.get(t_id)
            return t.progress()
        def shutdown(self, t_id):
            t = self.get(t_id)
            if t.is_suspended():
                t.resume()
            t.safe_kill()
            t.wait()
        def exit(self):
            m_set = self.__active.union(self.__q)
            for t in m_set:
                if t.is_suspended():
                    t.resume()
                t.safe_kill()
                t.wait()
            self.__log.warning("-", extra={"descriptor": "", "id": "", "pagenum": "", "pagesize": ""})
        def log_parse(self):
            with open("log" + os.sep + "activity.log", 'r') as qfile:
                qfile.seek(0, os.SEEK_END)
                pos = qfile.tell()
                line = ''
                while pos >= 0:
                    qfile.seek(pos)
                    next_char = qfile.read(1)
                    if next_char == "\n":
                        yield line[::-1]
                        line = ''
                    else:
                        line += next_char
                    pos -= 1
                yield line[::-1]
        def app_recovery(self,main):
            pass
            #for record in self.log_parse():
            #    rec_arr = record.split(" ")
            #    if rec_arr[0] == "-": break
            #    if rec_arr[0] == "ic": pass
            #    elif rec_arr[0] == "r": pass
        def init_save(self,state, type, save_fp):
            if type == "Production Headers":
                state = abbrev(state).name
                type = "prodh"
            elif type == "Permits":
                type = "perm"
            else:
                type = "prodm"
            path = save_fp + "/" + state + type + ".csv"
            if not os.path.exists(path):
                return False
            for rec in self.log_parse():
                rec_arr = rec.split(" ")
                if rec_arr[0] == "c" and rec_arr[1] == path:
                    return False
                if rec_arr[0] == "ic" and rec_arr[1] == path:
                    try:
                        self.prev_save = self.__SaveState(rec_arr[2], int(rec_arr[3]), int(rec_arr[4]))
                        return True
                    except ValueError:
                        return False
        def get_save_id(self):
            return self.prev_save.id()
        def get_save_page(self):
            return self.prev_save.pn()
        def get_save_size(self):
            return self.prev_save.ps()
        def app_crash(self):
            log_gen = self.log_parse()
            for c in log_gen:
                last_char = c.strip()
                if last_char != '':
                    return last_char != "-"
            return False
        def id(self):
            return repr(self)
        def return_fileCnt(self, fp):
            lines = 0
            with open(fp, 'rb') as fsrc:
                buf_s = 1024 * 1024
                rf = fsrc.raw.read
                buf = rf(buf_s)
                while buf:
                    lines += buf.count(b'\n')
                    buf = rf(buf_s)
            return lines
        def split_nd_cpy(self, fp, tmp_dir, split_size):
            tmp_dir += os.sep
            loc = []
            bse = os.path.basename(fp)
            with open(fp, 'r') as fsrc:
                header = next(fsrc) #Remove header
                cnt = 0
                pass_num = 1
                fdst = open(tmp_dir + bse[:bse.rindex(".")] + str(pass_num)+ ".csv", 'w')
                fdst.write(header)
                loc.append(tmp_dir + bse[:bse.rindex(".")] + str(pass_num)+ ".csv")
                for line in fsrc:
                    fdst.write(line)
                    cnt += 1
                    if cnt % split_size == 0:
                        pass_num += 1
                        fdst.close()
                        fdst = open(tmp_dir + bse[:bse.rindex(".")] + str(pass_num) + ".csv", 'w')
                        fdst.write(header)
                        loc.append(tmp_dir + bse[:bse.rindex(".")] + str(pass_num) + ".csv")
                fdst.close()
            return loc
