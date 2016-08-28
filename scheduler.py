from collections import namedtuple
from multiprocessing import SimpleQueue, Process, Condition, Pipe
from time import sleep, time
from heapq import heappush, heappop

class Task(namedtuple('Task', 'time, fn, args')):
    def __repr__(self):
        return "%s(%s) @%s" % (self.fn.__name__, self.args, self.time)

class MultiProcessScheduler:
    def __init__(self):
        self.cond = Condition()  # default to RLock

        # If duplex is False then the pipe is unidirectional
        # conn1 for receiving messages and conn2 for sending messages.
        conn1, conn2 = Pipe(duplex=False)
        self.connREAD = conn1
        self.connWRITE = conn2

        # a holder to the closest task to execute
        # it is not safe to access this variable directly as
        # there might be data on the pipe, use self.__getClosestTask()
        self._closestTask = None

        # multiprocessing.Queue is used here to exchange task between the add
        # call and the service running __run() method
        self.queue = SimpleQueue()

        # dummy Process, the correct one will be created when the first
        # task is added
        self.service = Process()

    # TODO create destructor to avoid leaving with items on queue

    def __getClosestTask(self):
        '''
        return the closest task to execute (i.e., top on pq)
        '''
        if self.connREAD.poll():
            ret = None
            while self.connREAD.poll():
                ret = self.connREAD.recv()
            self._closestTask = ret
            print("[conn] closestTaskUpdate: ", self._closestTask)
        return self._closestTask

    def add(self, task):
        if type(task) is not Task:
            raise TypeError
        self.queue.put(task)
        if not self.service.is_alive():
            # it seams that Process.run() is a blocking call
            # so the only way to re-run the process is to create another one
            self.service = Process(
                target=MultiProcessScheduler.__run,
                args=(self.cond, self.queue, self.connWRITE),
                daemon=False
            )
            self.service.start()
        else:
            # notify the condition variable if the new task has the
            # closest execution time
            closestTask = self.__getClosestTask()
            if closestTask and task.time < closestTask.time:
                self.cond.acquire()
                self.cond.notify()
                self.cond.release()

    @staticmethod
    def __run(cond, queue, conn):
        tasksQueue = []
        print("[run] starting", queue.empty())
        while True:
            # remove tasks from queue and add to
            # internal priorityQueue (tasksQueue)
            while not queue.empty():
                task = queue.get()
                heappush(tasksQueue,task)
                print("[run] adding task to pq: ", task)

            # if there are task on the priority queue,
            # check when the closest one should be runned
            if tasksQueue:
                etime, _, _ = task = tasksQueue[0]
                now = time()
                if etime < now:
                    # only pop before running
                    # if a task is not being running in a given time,
                    # the next this loop runs that task might not be the
                    # closest one
                    _, fn, args = heappop(tasksQueue)
                    print("[run] running:", task)
                    p = Process(target=fn, args=args, daemon=False)
                    p.start()
                else:
                    delay = etime - now

                    print("[run] sleeping for ", delay, task)

                    # send the closest task to the pipe
                    conn.send(task)

                    cond.acquire()
                    cond.wait(timeout=delay)

            if not tasksQueue and  queue.empty():
                # only stop the service if there are no task anwhere
                break
        print("[run] done")


if __name__ == "__main__":
    # TODO move this to test/
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
    sleep(1)

    print("[main] adding fn(2)")
    s.add(Task(time() + 2, fnfoo, "2"))
