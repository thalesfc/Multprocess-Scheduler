# TODO list
# 2 - use a main process and a multiprocessing.Queue to exchange new adds
# 3 - internally use a priority queue to keep track and order of tasks
# 4 - use a conditional variable to delay between execution
# 5 - spawn daemon-processes
# 6 - there should be no "run()" method, adding a new task fires the service

from collections import namedtuple
from multiprocessing import SimpleQueue, Process, Condition
from time import sleep, time

Task = namedtuple('Task', 'time, fn, args')

class MultiProcessScheduler:
    def __init__(self):
        self.cond = cond = Condition()  # default to RLock
        self.queue = queue = SimpleQueue()
        self.service = Process(
            target=self.__run,
            args=(cond,queue),
            daemon=False
        )
        self.service.start()

    def add(self, task):
        if type(task) is not Task:
            raise TypeError
        self.queue.put(task)

        # TODO call __run in case it is not alive
        # TODO if this new task is the closedt one, call __run

    def __run(self, cond, queue):
        sleep(5) # TODO remove
        print("[run] starting", queue.empty())
        while True:
            while not queue.empty():
                task = queue.get()
                print("[run] running:", task)
                # TODO add the new task to a priority queue
            # TODO create logic of priority queue
            break # TODO create break logic
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
    s.add(Task(time() + 5, fnfoo, "5"))
    s.add(Task(time() + 7, fnfoo, "7"))
