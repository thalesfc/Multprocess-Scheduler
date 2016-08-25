# TODO list
# 1 - the spawmed process is a not daemon process
# 2 - use a main process and a multiprocessing.Queue to exchange new adds
# 3 - internally use a priority queue to keep tasks
# 4 - use a conditional variable to delay between execution
# 5 - spawn daemon-processes

from multiprocessing import SimpleQueue, Process, Condition
from time import sleep

class MultiProcessScheduler:
    def __init__(self):
        self.cond = cond = Condition()  # default to RLock
        self.service = Process(target=self.__run, args=(cond,), daemon=False)
        self.service.start()

    def __run(self, cond):
        sleep(5)
        print("ruuning")


if __name__ == "__main__":
    # test 1  - the spawmed process is a not daemon process
    t1 = MultiProcessScheduler()
    print("done")
