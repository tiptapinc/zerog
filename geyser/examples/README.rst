***************
Geyser Examples
***************

Start local instance of beanstalkd
----------------------------------
Beanstalk will default to using port 11300 locally.

.. code-block:: shell

    # start daemon
    sh scripts/local_beanstalkd.sh start

    # list tubes
    sh scripts/local_beanstalkd.sh show

    # stop daemon
    sh scripts/local_beanstalkd.sh stop


Start local instance of couchbase
----------------------------------
Download Couchbase and start the application.

Couchbase will default to using port 8091 locally.


Basic Example
=============
The most basic set up with a single job on a single queue.

Run Locally
-----------
Make sure both beanstalk and couchbase are running locally.

Start up the geyser server.

.. code-block:: shell

    python server.py


Open a python console to create and queue an instance of the job.

.. code-block:: python

    import registry
    import datastore_configs

    from examples.basic_example import make_basic_job

    registry.build_registry()
    datastore_configs.set_datastore_globals()

    basic_job_values = {
        'fieldOne': 'one',
        'fieldTwo': 2,
        'fieldThree': 3,
    }
    job = make_basic_job(values=basic_job_values)
    job.enqueue()
    output = dict(uuid=job.uuid)

The job should complete successfully. A new document should show up in couchbase representing the job.
