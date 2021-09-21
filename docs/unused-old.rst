Reference
=========

zerog.BaseJob
-------------

All zeroG jobs must inherit from BaseJob. BaseJob includes all of the base attributes required for zeroG to identify, save, run, and report on a job. BaseJob includes the methods for saving jobs to the datastore, loading jobs from the datastore, instantiating jobs based on the data from the datastore, enqueuing jobs on their respective queues, and updating job attributes.

*MyJob*
    MyJob inherits from :code:`zeroG.BaseJob` to have all of the required base functionality and attributes. The developer can add additional attributes in :code:`__init__` as required for a job's function as well as the job type and queue name for the specific job. The inheriting class must overwrite the :code:`run` method.

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

    ZeroG has a few special error types that can be raised in the :code:`run` method.

    *zeroG.WFErrorFinish*
        job execution is terminated, and job is not requeued
    *zeroG.WFErrorContinue*
        job execution is terminated, but the job is requeued within its retry limit for re-execution
*MyJobSchema*
    MyJobSchema explicitly declares and provides validation for the inputs to MyJob. In order to use the schema validation provided by the marshmallow package, jobs must be instantiated via the :code:`make_base_job` method.

    .. code-block:: python

        # Use this creator function to create a job where the schema gets validated
        def make_my_job(values={}):
            return zeroG.jobs.make_base_job(values, MY_JOB_TYPE)
*Registry*
    Registry is the current mechanism by which ZeroG keeps track of jobs and queues. In order to add a job to the registry, add it to :code:`zeroG.registry.JOB_MODULES`.

    .. code-block:: python

        zeroG.registry.JOB_MODULES = zeroG.registry.JOB_MODULES + [
            "zeroG.examples"
        ]


Using Job Log to Track Job
--------------------------

``zeroG.job_log`` contains a set of helper functions and variables that allow jobs to report on their progress, record information, raise errors, and provide heartbeats to keep long running jobs alive.

*set_completeness(completeness, enforceMinInterval=False)*
    Manually set the completeness of a job.

    :completeness:
        float

        The completeness of a job measured by the developer's definition. A common implementation is a scale from 0 to 1 with 0 being not started and 1 being fully completed.
    :enforceMinInterval:
        boolean

        If True, requires updates to be at least 2 seconds apart.

*track_completeness(start, end, intervals)*
    Set up job log to be able to track completeness in set intervals.

    :start:
        float

        The completeness range start.
    :end:
        float

        The completeness range end.
    :intervals:
        float

        The number of intervals for completeness tracking that will occur between start and end. For example, a job with 5 equal steps might have 5 intervals with a start of 0.0 and an end of 1.0.

*increment_completeness()*
    Use together with ``track_completeness`` to take advantage of automatic completeness calculations. Each call to ``increment_completeness`` will increment the current interval for a job's completeness.

    Defaults to :code:`start=0.0`, :code:`end=1.0`, :code:`interval=1`.

*info(msg)*
    Record a message on a job.

    :msg:
        string

        Message to be associated with the job. This message will be saved to the job datastore entry along with a timestamp.

*error_log_only(msg)*
    Log an error that occurred during execution of the job. This error will stop immediate execution of the job, but will allow the job to be requeued for further retries.

    :msg:
        string

        Error message to be logged. This message will not be associated with the job datastore entry.

*error_continue(errorCode, msg)*
    Record an error on a job. This error will stop immediate execution of the job, but will allow the job to be requeued for further retries.

    :errorCode:
        int

        Error code for this particular error.
    :msg:
        string

        Message to be associated with the job. This message will be saved to the job datastore entry along with a timestamp.

*error_finish(errorCode, msg)*
    Record an error on a job. This error will stop immediate execution of the job, and the job will be removed from the queue.

    :errorCode:
        int

        Error code for this particular error.
    :msg:
        string

        Message to be associated with the job. This message will be saved to the job datastore entry along with a timestamp.

Creating a New Handler
----------------------

Endpoint handlers are the main way of creating and enqueueing ZeroG jobs. A handler that only enqueues jobs can be implemented very simply. The key point is that the handler needs to create the correct job with the correct parameters.

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

    import zeroG.jobs
    import zeroG.registry.JOB_MODULES

    from marshmallow import fields

    MY_JOB_TYPE = "my_job_type"


    class MyJobSchema(zeroG.jobs.BaseJobSchema):
        fieldOne = fields.String()


    class MyJob(zeroG.jobs.BaseJob):
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
        return zeroG.jobs.make_base_job(values, MY_JOB_TYPE)

    zeroG.registry.JOB_MODULES = zeroG.registry.JOB_MODULES + [
        "jobs.my_job"
    ]


Skeleton Code for MyJobHandler
------------------------------

:code:`handlers/my_job_handler.py`

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

            job = make_my_job(values=params)
            job.enqueue()

            output = dict(uuid=job.uuid)
            self.write("%s\n" % output)

            self.set_status(200)
            self.finish()

See examples_ folder for examples of the ZeroG system.


Glossary
========
*Job*
    A blueprint for performing work. Jobs can be defined and customized by the developer. Workers will pick up jobs from their respective queues and executed, performing the work dictated by the job. Jobs are stored in a database to track their progress, results, and errors.
*Job Schema*
    The predefined attributes for a job. These are primarily implemented for code readability and job input validation.
*Queue*
    A beanstalk tube on which jobs for that queue type will be inserted. Workers watch the tubes and pick up jobs as they have capacity.
*Worker*
    A process that picks up a job from a queue, instantiates the job, and runs it.
*Handler*
    A Tornado abstraction that is used to create and enqueue jobs based on API calls.


.. _examples: https://github.com/tiptapinc/zeroG/tree/master/zeroG/examples



        
The flow of information in ZeroG is shown at a high level in the following diagram::


    _____________________                  ____________                        _______________
    ||         ||          ||                  ||        ||                        ||           ||
    || Tornado || Handlers || <-- request      || zeroG || --> save job data -->  || datastore ||
    ||         ||          || --> make_job --> ||        || <-- fetch job data <-- ||           ||
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
                                       ||  - init job  || <---------------
                                       ||  - job.run() ||
                                       ||              ||
                                       ------------------


A developer creates an application with an API layer with endpoints corresponding to jobs. A :code:`POST` request to one of thse endpoints will kick off the job queueing process. The job corresponding with that endpoint will be created, saved to the datastore, and its uuid added to the corresponding beanstalk tube ("queue"). Workers watching these queues will pick up jobs as they come in. The workers will pull the correct job information from the datastore by job uuid, instantiate the job by its job type, and then run the job. Any updates that occur during the execution of the job, such as progress updates, will be intermittenly saved to the datastore, and so the job data can be pulled at anytime by a watcher to keep track of the progress of the job.


