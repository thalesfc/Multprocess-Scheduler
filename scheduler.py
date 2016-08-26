from collections import namedtuple
from multiprocessing import SimpleQueue, Process, Condition
from time import sleep, time
from heapq import heappush, heappop

Task = namedtuple('Task', 'time, fn, args')

class MultiProcessScheduler:
    def __init__(self):
        self.cond = Condition()  # default to RLock

        # TODO start a pipe to have the most recent task

        # multiprocessing.Queue is used here to exchange task between the add
        # call and the service running __run() method
        self.queue = SimpleQueue()

        # dummy Process, the correct one will be created when the first
        # task is added
        self.service = Process()

    # TODO create destructor to avoid leaving with items on queue

    def add(self, task):
        if type(task) is not Task:
            raise TypeError
        self.queue.put(task)

        # TODO read the pipe to get the recent element
        # TODO if this new task is the closedt one, call __run
        if not self.service.is_alive():
            # it seams that Process.run() is a blocking call
            # so the only way to re-run the process is to create another one
            self.service = Process(
                target=MultiProcessScheduler.__run,
                args=(self.cond, self.queue),
                daemon=False
            )
            self.service.start()

    @staticmethod
    def __run(cond, queue):
        tasksQueue = []
        print("[run] starting", queue.empty())
        while True:
            # remove tasks from queue and add to
            # internal priorityQueue (tasksQueue)
            while not queue.empty():
                task = queue.get()
                heappush(tasksQueue,task)
                print("[run] adding task to list: ", task)

            # if there are task on the priority queue,
            # check when the closest one should be runned
            if tasksQueue:
                etime, _, _ = tasksQueue[0]
                now = time()
                # TODO use a pipe to send the most recent task
                if etime < now:
                    # only pop before running
                    # if a task is not being running in a given time,
                    # the next this loop runs that task might not be the
                    # closest one
                    _, fn, args = heappop(tasksQueue)
                    print("[run] running:", task)
                    fn(*args) # TODO spawn new process?
                else:
                    delay = etime - now
                    print("[run] sleeping for ", delay)
                    cond.acquire()
                    cond.wait(timeout=delay)

            if not taskQueue and  queue.empty():
                # only stop the service if there are no task anwhere
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
    print("[main] adding fn(9)")
    s.add(Task(time() + 9, fnfoo, "9"))

    sleep(3)

    print("[main] adding fn(1)")
    s.add(Task(time() + 1, fnfoo, "1"))
    print("[main] adding fn(2)")
    s.add(Task(time() + 2, fnfoo, "2"))

    s.cond.acquire()
    s.cond.notify()
    s.cond.release()
