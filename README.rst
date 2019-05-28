******
Geyser
******

Geyser is a queueing system specifically designed to accomodate long running jobs in a maintainable fashion.

Geyser runs using tornado to handle API requests and beanstalkd to handle queueing.

.. contents:: Table of Contents

Overview
========

Geyser is a queueing system that allows developers to abstract out the complicated and common problem of running background jobs in a maintainable fashion. The basis of Geyser is that jobs should be reliable, take an arbitrary amount of time, and have the ability to report on their progress.

The flow of information in Geyser is shown at a high level in the following diagram::

    _____________________                  ____________                        _______________
    ||     ||          ||                  ||        ||                        ||           ||
    || App || Handlers || <-- request      || geyser || --> save job data -->  || datastore ||
    ||     ||          || --> make_job --> ||        || <-- fetch job data <-- ||           ||
    ---------------------                  ------------                        ---------------
                                                |                                    /
                                               \|/                                  /
                                          _____________                            /
                                          | beanstalk |                           /
                                          |   tube    |                          /
                                          -------------                         /
                                                |                              /
                                               \|/                            /
                                       __________________                    /
                                       ||              ||                   /
                                       || BaseWorker   ||                  /
                                       ||  - init job  || <----------------
                                       ||  - job.run() ||
                                       ||              ||
                                       ------------------


A developer can create an application that has an API layer with endpoints corresponding to jobs. A :code:`POST` request to one of thse endpoints will kick off the job queueing process. The job corresponding with that endpoint will be created, saved to the datastore, and its uuid added to the corresponding beanstalk tube ("queue"). Workers watching these queues will pick up jobs as they come in. The workers will pull the correct job information from the datastore by job uuid, instantiate the job by its job type, and then run the job. Any updates that occur during the execution of the job, such as progress updates, will be intermittenly saved to the datastore, and so the job data can be pulled at anytime by a watcher to keep track of the progress of the job.


Using Geyser
============

Creating a New Job
------------------

All geyser jobs must inherit from BaseJob. BaseJob includes all of the base attributes required for geyser to identify, save, run, and report on a job. BaseJob includes the methods for saving jobs to the datastore (currently Couchbase), loading jobs from the datastore, instantiating jobs based on the data from the datastore, enqueuing jobs on their respective queues, and updating job attributes.

New jobs can be created as follows:


MyJob
^^^^^
MyJob inherits from :code:`geyser.BaseJob` to have all of the required base functionality and attributes. The developer can add additional attributes in :code:`__init__` as required for a job's function as well as the job type and queue name for the specific job. The inheriting class must overwrite the :code:`run` method.

.. code-block:: python

   def run(self):
        """
        fill in with custom functionality

        this method must return a tuple consisting of:
           - the time (in epoch seconds) at which to resume
             consuming from the queue

           - a boolean indicating whether the job should be
             re-queued for further processing
        """
        return time.time(), False

Geyser has a few special error types that can be raised in the :code:`run` method.

    :code:`geyser.WFErrorFinish`: job execution is terminated, and job is not requeued

    :code:`geyser.WFErrorContinue`: job execution is terminated, but the job is requeued within its retry limit for re-execution


MyJobSchema
^^^^^^^^^^^
MyJobSchema explicitly declares and provides validation for the inputs to MyJob. In order to use the validation provided by the marshmallow package, jobs must be instantiated via the :code:`make_base_job` method.

.. code-block:: python

    # Use this creator function to create a job where the schema gets validated
    def make_basic_job(values={}):
        return geyser.jobs.make_base_job(values, MY_JOB_TYPE)


Registry
^^^^^^^^
Registry is the current mechanism by which Geyser keeps track of which jobs and queues it needs to be aware of. In order to add a job to the registry, add it to :code:`geyser.registry.JOB_MODULES`.

.. code-block:: python

    geyser.registry.JOB_MODULES = geyser.registry.JOB_MODULES + [
        "geyser.examples"
    ]


Creating a New Handler
----------------------

Endpoint handlers are the main way of creating and enqueueing Geyser jobs. A handler that only enqueues jobs can be implemented very simply. The key point is that the handler needs to create the correct job with the correct parameters.

.. code-block:: python

    def post(self):
        params = tornado.escape.json_decode(self.request.body)
        ...
        job = make_my_job(values=params)
        job.enqueue()
        ...


Examples
========

Skeleton Code for MyJob
-----------------------

:code:`jobs/my_job.py`

.. code-block:: python

    import time

    import geyser.jobs
    import geyser.registry.JOB_MODULES

    from marshmallow import fields

    MY_JOB_TYPE = "my_job_type"


    class MyJobSchema(geyser.jobs.BaseJobSchema):
        fieldOne = fields.String()


    class MyJob(geyser.jobs.BaseJob):
        JOB_TYPE = MY_JOB_TYPE
        SCHEMA = MyJobSchema
        QUEUE_NAME = 'my_job'

        def __init__(self, **kwargs):
            super(BasicJob, self).__init__(**kwargs)

            self.fieldOne = kwargs.get('fieldOne')

        def run(self):
            """
            fill in with custom functionality

            this method must return a tuple consisting of:
               - the time (in epoch seconds) at which to resume
                 consuming from the queue

               - a boolean indicating whether the job should be
                 re-queued for further processing
            """
            return time.time(), False


    # Use this creator function to create a job where the schema gets validated
    def make_my_job(values={}):
        return geyser.jobs.make_base_job(values, MY_JOB_TYPE)

    geyser.registry.JOB_MODULES = geyser.registry.JOB_MODULES + [
        "jobs.my_job"
    ]


Skeleton Code for MyJobHandler
------------------------------

:code:`handlers/my_handler.py`

.. code-block:: python

    class MyJobHandler(tornado.web.RequestHandler):
        def get(self, argsDict):
            '''
            Get the status of a job.
            '''
            self.set_status(200)
            self.finish()

        def post(self):
            '''
            Kick off a MyJob.
            '''
            params = tornado.escape.json_decode(self.request.body)

            log.info(f'kicking off basic job with args: {params}')

            job = make_my_job(values=params)
            job.enqueue()

            output = dict(uuid=job.uuid)
            self.write("%s\n" % output)

            self.set_status(200)
            self.finish()

See examples_ folder for examples of the geyser system.


Glossary
========
* *Job*: a blueprint for performing work. Jobs can be defined and customized by the developer. Workers will pick up jobs from their respective queues and executed, performing the work dictated by the job. Jobs are stored in a database to track their progress, results, and errors.
* *Job Schema*: the predefined attributes for a job. These are primarily implemented for code readability and job input validation.
* *Queue*: a beanstalk tube on which jobs for that queue type will be inserted. Workers watch the tubes and pick up jobs as they have capacity.
* *Worker*: a process that picks up a job from a queue, instantiates the job, and runs it.
* *Handler*: a Tornado abstraction that is used to create and enqueue jobs based on API calls.


.. _examples: https://github.com/tiptapinc/geyser/tree/master/geyser/examples
