***************
Geyser Examples
***************

.. contents:: Table of Contents


Set Up Development Environment
===============================

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

Some other useful beanstalkd commands.

.. code-block:: shell

    # display beanstalkd stats
    echo "stats" | nc -c localhost 11300

    # list active tube names
    echo "list-tubes" | nc -c localhost 11300

    # display stats for a specific tube
    echo "stats-tube queue_name" | nc -c localhost 11300


Start local instance of couchbase
----------------------------------
Download Couchbase and start the application.

Couchbase will default to using port 8091 locally.


Basic Example
=============
The most basic set up with a single job on a single queue.

Run Locally with API Server
---------------------------
This will boot up both the geyser server and an API server to handle HTTP requests for kicking off jobs.

Make sure both beanstalk and couchbase are running locally.

Start up the geyser server.

.. code-block:: shell

    python server.py


Start up the API server.

.. code-block:: shell

    python example_server.py


Make a request to /basic/example.

.. code-block:: shell

    curl http://localhost:8880/basic/example --data '{"fieldOne":"ONE", "fieldTwo":2, "fieldThree":3}'

Check on the status of the job with beanstalkd.

.. code-block:: shell

    # display stats for a specific tube
    echo "stats-tube basic_job" | nc -c localhost 11300

Run Locally with the Console
----------------------------
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
