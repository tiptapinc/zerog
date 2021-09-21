.. ZeroG documentation master file, created by
   sphinx-quickstart on Fri Sep 17 15:52:06 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ZeroG - Simple & Reliable Job Processing
========================================

ZeroG is a lightweight and reliable python job processing system. It allows developers to abstract out the complicated and common problem of running background jobs in a maintainable fashion. The basis of ZeroG is that jobs should be reliable and resilient, take an arbitrary amount of time, and have the ability to report on their progress.

Zerog is designed so that job implementers can focus on the functionality of their jobs, without worrying about the overhead of job management. The simplest jobs can simply subclass zerog.BaseJob and implement all of their logic in the ``run`` method.

ZeroG can be combined with Spacewalk to add an auto-generated, discoverable heirarchy of REST endpoints and associated job parameter schemas.

Built-in ZeroG functionality includes:

- A REST interface to initiate & query jobs
- Parameter validation for job creation
- Error/exception handling
- Job logging
- Flexible capacity management

ZeroG has the following key dependencies

- Tornado Web Server for its REST API
- Marshmallow for schema definition, validation, and serialization/deserialization.
- A queueing server. The base ZeroG implementation uses the Beanstalkd queue
- A persistent key/value store. The base ZeroG implementation uses the Couchbase NoSQL database.

Overview
========

.. toctree::
   :maxdepth: 2

   basics
   example

API Reference
=============

.. toctree::
   :maxdepth: 2

   server
   jobs

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`