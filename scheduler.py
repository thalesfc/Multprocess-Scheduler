# TODO list
# 4 - use a conditional variable to delay between execution
# 5 - spawn daemon-processes
# 6 - there should be no "run()" method, adding a new task fires the service

from collections import namedtuple
from multiprocessing import SimpleQueue, Process, Condition
from time import sleep, time
from heapq import heappush, heappop

Task = namedtuple('Task', 'time, fn, args')

class MultiProcessScheduler:
    def __init__(self):
        self.cond = cond = Condition()  # default to RLock
        self.queue = queue = SimpleQueue()
        self.service = Process(
            target=MultiProcessScheduler.__run,
            args=(cond,queue),
            daemon=False
        )
        self.service.start()

    # TODO create destructor to avoid leaving with items on queue

    def add(self, task):
        if type(task) is not Task:
            raise TypeError
        self.queue.put(task)

        # TODO call __run in case it is not alive
        # TODO if this new task is the closedt one, call __run

    @staticmethod
    def __run(cond, queue):
        sleep(5) # TODO remove
        tasksQueue = []
        print("[run] starting", queue.empty())
        while True:
            while not queue.empty():
                task = queue.get()
                heappush(tasksQueue, task)
            # TODO create logic of priority queue
            while tasksQueue:
                etime, _, _ = tasksQueue[0]
                now = time()
                if etime < now:
                    _, fn, args = task = heappop(tasksQueue)
                    print("[run] running:", task)
                    fn(*args) # TODO spawn new process?
                else:
                    delay = etime - now
                    print("[run] sleeping for ", delay)
                    sleep(delay) # TODO change to cond. variable
            if queue.empty():
                break
        print("[run] done")


if __name__ == "__main__":
    def fnfoo(arg): print(arg)

    # test 1  - the spawmed process is a not daemon process
    s = MultiProcessScheduler()
    print("[main] scheduler created")

    # test 2.1 - wrong type of task should raise
    try:
        s.add('foo')
    except TypeError:
        pass
    else:
        raise Exception("add should only accept a Task")

    # test 2.2 - add a valid task
    print("[main] adding fn(5)")
    s.add(Task(time() + 9, fnfoo, "9"))
    s.add(Task(time() + 5, fnfoo, "5"))
    s.add(Task(time() + 7, fnfoo, "7"))
