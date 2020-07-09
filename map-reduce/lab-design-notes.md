### Flow Summary

- 1 master, multiple workers
- Master starts up:

  - Saves user-specified args (i.e. files to process, number of reduce outputs etc) in its internal
    state.
  - Master starts up its server and wait for workers to connect. (Workers
    register with master through REGISTER RPC)

- The master's main function will essentially create a master and keep looping
  until m.Done() is true.
- First thing the worker does when it starts up is it registers with the master.
- The master has an internal record of all registered workers, so that it can
  handout tasks and track their progress later.
-
