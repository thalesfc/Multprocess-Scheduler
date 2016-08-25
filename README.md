# MultiProcess-Scheduler

### Design implementation

1. The "service" process  has daemon=False, so it will stay alive after parent
process die
1. Use a multiprocessing.Queue to exchange tasks between main process and
"service" process
