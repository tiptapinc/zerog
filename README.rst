******
Geyser
******

Geyser is a queueing system specifically designed to accomodate long running jobs in a maintainable fashion.

Geyser runs using tornado to handle API requests and beanstalkd to handle queueing.


Examples
========
See examples_ folder for examples of the geyser system.


Key Components
==============
A quick diagram showing high level how the system works::

    _____________________              ____________                    _______________
    ||     ||          ||              ||        ||                    ||           ||
    || App || Handlers || <-- request  || geyser || --> save job data  || datastore ||
    ||     ||          || --> make_job ||        || <-- fetch job data ||           ||
    ---------------------              ------------                    ---------------
                                            |
                                           \|/
                                          _____
                                          | b |
                                          | e |
                                          | a |
                                          | n |
                                          | s |
                                          | t |
                                          | a |
                                          | l |
                                          | k |
                                          |   |
                                          | t |
                                          | u |
                                          | b |
                                          | e |
                                          -----
                                            |
                                           \|/
                                   __________________
                                   ||              ||
                                   || BaseWorker   ||
                                   ||  - init job  ||
                                   ||  - job.run() ||
                                   ||              ||
                                   ------------------

BaseJob
--------
BaseJob is the base class that all geyser jobs must inherit from.

In order to create a new job type with custom functionality, implement the following two custom classes:
1. MyJob, which inherits from :code:`geyser.BaseJob` and overwrites the :code:`run` method
2. MyJobSchema, which explicitly declares and provides validation for the inputs to MyJob

.. code-block:: python

    import time

    import geyser.jobs

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
    def make_basic_job(values={}):
        return geyser.jobs.make_base_job(values, MY_JOB_TYPE)

See examples_ for additional examples.

Glossary
========
* *Job*: a blueprint for performing work. Jobs can be defined and customized by the developer. Workers will pick up jobs from their respective queues and executed, performing the work dictated by the job. Jobs are stored in a database to track their progress, results, and errors.
* *Job Schema*: the predefined attributes for a job. These are primarily implemented for code readability and job input validation.
* *Queue*: a beanstalk tube on which jobs for that queue type will be inserted. Workers watch the tubes and pick up jobs as they have capacity.
* *Worker*: a process that picks up a job from a queue, instantiates the job, and runs it.
* *Handler*: a Tornado abstraction that is used to create and enqueue jobs based on API calls.


.. _examples: https://github.com/tiptapinc/geyser/tree/master/geyser/examples
