Basics
======

Block Diagram
-------------
The basic information flow for a execution of a ZeroG is shown in the following diagram::

              ________           _________________
              | REST | _________ |    Server     |
              |  API |           | (Tornado App) |
              --------           -----------------
                                        |
                                        |
                                   ------------
                                   | Handlers |
                                   ------------
                                    /        \
                                   /          \
                                  /            \
                                 /              \
                                /                \
                               /                  \
                              /                    \
              ________________      __________      _______________
              |    Queue     | ____ |        | ____ |  Datastore  |
              | (Beanstalkd) |      | Worker |      | (Couchbase) |
              ----------------      ----------      ---------------
                                         |          /
                                         |         /
                                         |        /
                                         |       /
                                         |      /
                                         |     /
                                         |    /
                                    __________
                                    |        |
                                    |  Job   |
                                    ----------


Job Initiation
--------------
A new job is created by making an HTTP POST request to the ``run job`` endpoint

- job parameters are JSON-encoded in the request body
- the POST is passed to a zerog request handler:
- the handler validates the job parameters
- the handler instantiates a new job using those parameters
- the handler persists the new job to the datastore
- the handler enqueues the job's UUID
- the handler returns the job's UUID in the response to the HTTP POST

Job Execution
-------------
A zerog worker polls the queue and is returned a job UUID

- the worker uses the UUID to retrieve the job data from the datastore and  instantiate the job
- the worker calls the job's ``run`` method to execute the job
- if there is an unhandled exception while running the job, the worker handles the  exception and decides whether to re-queue the job or declare it finished
- if the job completes successfully, the worker examines the result and either  declares the job finished, or re-queues the job for further execution
- the job's state is persisted after each of the above cases

Job Monitoring
--------------
jobs can be monitored & queried during and after their execution by making HTTP GET requests with the job's UUID as a parameter

- a request to the ``progress`` endpoint returns the job's status and completion    percentage. A resultCode of -1 indicates that the job is still running
- a request to the ``info`` endpoint returns the job's status and completion    percentage, as well as any events, errors, and warnings that the job has accumulated
- a request to the ``get data`` endpoint returns the output data for a completed job

