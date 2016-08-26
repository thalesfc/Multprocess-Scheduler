# MultiProcess-Scheduler

### Design implementation

1. The "service" process  has daemon=False, so it will stay alive after parent
process die
1. A multiprocessing.Queue is used to exchange tasks between main process and
"service" process
1. __run() method internally uses a priorityQueue to keep track of tasks
execution order
1. users should not run __run(), instead, everytime a new task is added we
check for the state of service (i.e., __run()) and re-spawn a new process if
necessary
