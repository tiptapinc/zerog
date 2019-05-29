*****
Notes
*****
Notes that came up during development. Some of these can maybe be pulled out and added to a README or some other document. Others can be deleted after they've been addressed.

.. contents:: Table of Contents

General Thoughts
================

* Pieces of the system are optimized for Couchbase as the datastore

  - ``make_key`` in ``BaseJob`` uses ``self.DOCUMENT_TYPE`` to make queries requesting all jobs easier in Couchbase

* Need to create a strategy to allow the use of different datastores

  - having a global ``datastore_config.DATASTORE`` is difficult to mock in tests

* Which existing constants should be changed to be configurable attributes on jobs?

  - ``base_worker.MAX_TIMEOUTS``
  - ``base_worker.MAX_RESERVES``
  - ``job_log.MIN_UPDATE_INTERVAL``

* The current keep alive ``WATCHDOG`` is kind of sketchy

Component-Specific Thoughts
===========================

BaseJob
-------

* ``BaseJob.record_change`` will update the attributes on the base job instance even if the save to the datastore fails
* Job parameters are only validated if the job is instantiated via ``make_base_job``

  - Can this be assumed or enforced?

* Can we add functionality to make it possible to kill a job at any point in its execution?
* ``BaseJob.record_change`` is implemented with the assumption that the datastore being used is Couchbase

  - How much of it is Couchbase dependent?
  - How much can be generalized?


Job Log
-------
* Using the global ``CURRENT_JOB`` makes it tricky when there are multiple jobs

  - Jobs in parallel is impossible
  - One job creating and waiting on the result of another job means that the parent job must know to reset the value of ``CURRENT_JOB``


Registry
--------
* Current implementation is cumbersome

  - ``geyser.server`` has to know about all BaseJob subclasses (well actually mostly just the queue names...) to create the correct queues and workers
  - If two jobs have the same queue name, that queue will be added twice, and two workers will be created to watch the one queue
  - External (developer) API server also has to register all BaseJob subclasses
  - Not clear that this is even possible if Geyser is made into a package


Datastore
---------
* Currently implements some functionality that is very specific to Couchbase

  - For example ``set_with_cas``

* Will need additional datastore support for alternative datastores


Geyser Queue
------------
* I think ``work_queue`` can be deleted at this point

  - Uses ``beanstalkt`` which is locked on an outdated version of tornado
  - Enables *supposedly* asynchronous beanstalkd behavior
  - Geyser (and python) are inherently synchronous, so ``beanstalkt`` is not necessary

* ``queue_handler`` can potentially replace ``sync_queue`` at some point also I think

Testing
=======
Tests are written with pytest. In order to use the modules in the main code folder, install geyser locally in edit mode.

In the root directory run:

.. code-block:: shell

    pip install -e .

Imports are done by appending `geyser` to every import.

.. code-block:: python

    import geyser.jobs
