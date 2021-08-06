##v0.0.29
* don't raise exception for raise_warning_continue

##v0.0.29
* add raise_warning_continue method to jobs

## v0.0.28
* set queueJobId to -1 on failed enqueue

## v0.0.27
* fix 'BaseWorker' object has no attribute 'record_result'

## v0.0.26
* limit data returned in job.progress method
* add job.info method to return complete job info

## v0.0.25
* fix beanstalk client reconnect/retry

## v0.0.24
* update server state management
* fix issues with pipe buffer getting filled up
* update logging info
* make memory info in infomsg more concise
* include memory info in WorkerManager workerData

## v0.0.23
* suicide worker after running a job to return memory to OS

## v0.0.22
* clean up beanstalkc socket error handling

## v0.0.21
* detach from ctrl queue after sending message

## v0.0.20
* make Dockerfile & docker-compose file for testing
* fix tests
* add support for queue-based management

## v0.0.19
* change default queue TTR to 30 days

## v0.0.18
* changes to handle system restarts better

## v0.0.17
* rename data method to get_data
* set allow_nan=False when json dumping progress and get_data output

## v0.0.16
* add support for 'data' method so results don't need to be returned
  by the 'progress' method

## v0.0.15
* retry on couchbase timeouts

## v0.0.14
* Changed datastore & queue __init__ arguments to 'makeDatastore' and
  'makeQueue', which allows instantiation of datastore & queue objects
  in server & worker contexts
* Add Server.make_job, Server.get_job, and BaseWorker.get_job methods.
  Direct use of Registry.make_job and Registry.get_job is deprecated.
* Improve Server<->Worker messaging
* Add Server.stop_worker_polling, Server.start_worker_polling, and
  Server.stop_worker methods which all pass messages to the worker
* Recognize missing or zombie worker and restart
* Track runningJobUuid and add an error message to the running job if
  its execution was interrupted by something that caused a zombie or
  missing worker

## v0.0.13
* change datastore & queue args to makeDatastore & makeQueue

## v0.0.12
* allow connection kwargs when creating CouchbaseDatastore
		- operation_timeout
		- config_total_timeout
		- config_node_timeout

## v0.0.11
* always requeue on couchbase timeouts -- temp hack

## v0.0.10
* expect job's run method to return a (resultCode, delay) tuple
* handle non-tuple return values

## v0.0.9
* use atexit for exit cleanup instead of trapping SIGINT & SIGKILL

## v0.0.8
* log job completion

## v0.0.7
* switch all log levels to INFO
* log job info on run_job POST

## v0.0.6
* only handle SIGTERM, not SIGINT

## v0.0.5

* relocate 'tests' directory
* add imports to top-level __init__.py

## v0.0.4

* remove __init_subclass__ checks on class attribute overrides

## v0.0.3

* I forget

## v0.0.2

* I forget

## v0.0.1:

* I forget
