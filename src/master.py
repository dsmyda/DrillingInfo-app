from queue import Queue
import threading
import logging

MAX_RUNNING = 2

q = Queue()
active = set()
completed = set()
lock = threading.Lock()

handler = logging.FileHandler("activity.log")
handler.setFormatter(logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                                       "%Y-%m-%d %H:%M:%S"))
log = logging.getLogger("activity.log")
log.addHandler(handler)

def send(worker):
    q.put(worker)
    with lock:
        if(len(active) < MAX_RUNNING):
            worker.start()
            active.add(q.get())

def remove_active(worker):
    """ """

    if worker not in active: return

    with lock:
        active.remove(worker)
        completed.add(str(worker))
        if not q.empty():
            next = q.get()
            next.start()
            active.add(next)

def pause(worker_name):

    for worker in active:
        if str(worker) == worker_name:
            worker.suspend()

def resume(worker_name):

    for worker in active:
        if str(worker) == worker_name:
            worker.resume()

def get_status(worker_name):

    if worker_name in completed:
        return 4

    for worker in active:
        if str(worker) == worker_name:
            if worker.is_suspended():
                return 3
            return 1

    return 2
