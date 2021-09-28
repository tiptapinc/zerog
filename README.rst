*****
ZeroG
*****

.. contents:: Table of Contents

Introduction
============

ZeroG is a lightweight and reliable python job processing system. It allows developers to abstract out the complicated and common problem of running background jobs in a maintainable fashion. The basis of ZeroG is that jobs should be reliable and resilient, take an arbitrary amount of time, and have the ability to report on their progress.

Zerog is designed so that job implementers can focus on the functionality of their jobs, without worrying about the overhead of job management. The simplest jobs can simply subclass zerog.BaseJob and implement all of their logic in the ``run`` method.

ZeroG can be combined with Spacewalk to add an auto-generated, discoverable heirarchy of REST endpoints and associated job parameter schemas.

Built-in ZeroG functionality includes:

- A REST interface to initiate & query jobs
- Parameter validation for job creation
- Error/exception handling
- Job logging
- Flexible capacity management

Overview
========

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


Using ZeroG - A Simple Example
==============================

Create a Job Class
------------------
This example creates a job that will waste a specified amount of time, while randomly logging approximately 10 messages

.. code-block:: python

    from marshmallow import fields
    import random
    import time
    import zerog

    class WasteTimeJobSchema(zerog.BaseJobSchema):
        delay = fields.Integer()


    class WasteTimeJob(zerog.BaseJob):
        JOB_TYPE = "waste_time"
        SCHEMA = WasteTimeJobSchema

        def __init__(self, *args, **kwargs):
            super(WasteTimeJob, self).__init__(*args, **kwargs)
            self.delay = kwargs.get('delay', 30)

        def run(self):
            end = time.time() + self.delay
            logInterval = self.delay / 10

            while True:
                if time.time() > end:
                    break

                logDelay = (random.random() + 0.5) * logInterval
                time.sleep(logDelay)
                self.add_to_completeness(logDelay / self.delay)
                self.job_log_info(f"{end - time.time():.2f} seconds remaining")

            return 200, None

Create a ZeroG Service
----------------------
Creating a ZeroG service is as simple as creating a new :code:`zerog.Server` instance.

.. code-block:: python

    import tornado.ioloop
    import zerog

    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - "
               "%(message)s - [%(process)s:%(name)s:%(funcName)s]"
    )
    log = logging.getLogger(__name__)


    def make_datastore():
        return zerog.CouchbaseDatastore(
            "couchbase", "Administrator", "password", "test"
        )


    def make_queue(queueName):
        return zerog.BeanstalkdQueue("beanstalkd", 11300, queueName)


    handlers = [
        (f"/job/{zerog.JOB_TYPE_PATT}", zerog.RunJobHandler),
        (f"/progress/{zerog.UUID_PATT}", zerog.ProgressHandler),
        (f"/info/{zerog.UUID_PATT}", zerog.InfoHandler),
        (f"/data/{zerog.UUID_PATT}", zerog.GetDataHandler)
    ]

    server = zerog.Server(
        "myService",
        make_datastore,
        make_queue,
        [WasteTimeJob],
        handlers
    )
    server.listen(8888)
    tornado.ioloop.IOLoop.current().start()


Get It Now
==========

::


    $ pip install -e git+https://github.com/tiptapinc/zerog.git#egg=zerog


Documentation
=============

Full documentation is available at https://zerog.readthedocs.io/en/latest/ .

Requirements
============

ZeroG has the following key dependencies

- Tornado Web Server for its REST API
- Marshmallow for schema definition, validation, and serialization/deserialization.
- A queueing server. The base ZeroG implementation uses the Beanstalkd queue
- A persistent key/value store. The base ZeroG implementation uses the Couchbase NoSQL database.
- Python >= 3.6

