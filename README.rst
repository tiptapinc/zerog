******
Geyser
******

Key Components
==============

Base Job
--------
Base implementation of a *job* (where work is defined). Includes a base schema that contains the bare minimum of information necessary for a job to be processed.

Implements:
* *reload*: reload job data from database
*
*


Base Worker
-----------


Glossary
========
* *Job*: a blueprint for performing work. Jobs can be defined and customized by the developer. Workers will pick up jobs from their respective queues and executed, performing the work dictated by the job. Jobs are stored in a database to track their progress, results, and errors.
* *Job Schema*: the predefined attributes for a job. These are primarily implemented for code readability and job input validation.
* *Queue*: a beanstalk tube on which jobs for that queue type will be inserted. Workers watch the tubes and pick up jobs as they have capacity.
* *Worker*: a process that picks up a job from a queue, instantiates the job, and runs it.
* *Handler*: a Tornado abstraction that is used to create and enqueue jobs based on API calls.
