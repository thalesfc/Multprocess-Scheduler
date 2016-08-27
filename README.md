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
1. Use a condition variable to delay between execution, this way we can
wake up the service process in case of a more recent task
1. Use a pipe so that the service method can update with the closest
next task to execute (this way, if a new task with closest time is added
then the service is waked up)
