#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import json
import os
import traceback

import zerog.jobs

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

MAX_TIMEOUTS = 3
MAX_RESERVES = 3

POLL_INTERVAL = 2


class BaseWorker(object):
    """
    Reserve jobs from a queue, instantiate those jobs, and run them.

    Run as a child process of a Server class object.

    Communicate with the parent via a multiprocessing Pipe

    Args:
        datastore: Datastore object for persisting jobs.

        registry: JobRegistry object that maps job types to job classes

        queue: Queue for sharing jobs with workers

        conn: connection object created by multiprocessing.Pipe()
              Used to communicate with parent.
    """
    def __init__(self, datastore, queue, registry, conn, **kwargs):
        self.datastore = datastore
        self.registry = registry
        self.queue = queue
        self.conn = conn

        self.parentPid = os.getpid()

    def run(self):
        """
        Process() target function.

            - Communicates with parent server.
            - Polls job queue
            - Runs jobs
        """
        log.debug("starting worker.run()")

        self.runningJobs = True
        self.conn.send("READY")

        while True:
            # check if parent has sent a message
            #
            # blocks for POLL_INTERVAL seconds, so that determines the
            # rate at which the job queue is polled
            if self.conn.poll(POLL_INTERVAL) is True:
                msg = self.conn.recv().lower()
                if msg == "stop polling":
                    log.debug("received message: %s" % msg)
                    self.runningJobs = False
                elif msg == "start polling":
                    log.debug("received message: %s" % msg)
                    self.runningJobs = True
                elif msg == "die":
                    log.debug("received message: %s" % msg)
                    return

            # check if there is a job available in the job queue. Try to
            # run the job if so.
            #
            # catch DEADLINE_SOON exception here or in queue object?
            queueJob = self.queue.reserve(timeout=0)
            if queueJob:
                log.debug("running job: %s" % queueJob.body)
                self._process_queue_job(queueJob)

            # check if parent is still alive. Suicide if not.
            if self._check_parent() is False:
                break

    def _check_parent(self):
        # Return True if parent is still alive, False if not

        # This is not portable. Works on posix systems. Relies on
        # os.kill(pid, 0), which does nothing if the process exists and
        # throws an exception if it does not.
        try:
            os.kill(self.parentPid, 0)
        except OSError:
            return False
        else:
            return True

    def _process_queue_job(self, queueJob):
        # Handle a job that has been reserved from the queue. Try to
        # load and run the associated job. Handle all error conditions
        # associated with the job so a problematic job is unlikely to
        # crash the worker
        #
        # Args:
        #   queueJob: queue job object. Currently it is a beanstalkc.Job
        #
        # body of the queue job is just a uuid that we can use to retrieve
        # the full job
        uuid = json.loads(queueJob.body)
        stats = queueJob.stats()

        log.debug("job %s queue stats: %s" % (uuid, stats))

        try:
            job = None
            job = self.registry.get_job(uuid)

            # if job failed to load, release the queueJob back to the queue
            # and return to the main loop.
            if job is None:
                log.debug("failed to load job %s. Releasing queueJob" % uuid)
                queueJob.release(delay=10)
                return

            log.debug("loaded job: %s" % job)

            # if job has been retried too many times, return to main loop
            # without trying to run it. The job has been deleted from the
            # queue if _manage_retries returns True
            if self._manage_retries(queueJob, stats, job):
                log.debug("job %s too many retries. Deleted queueJob" % uuid)
                return

            # run the job by calling its run method
            resultCode = job.run()
            log.debug("job %s completed. resultCode: %s" % (uuid, resultCode))

            # delete the queueJob now that the actual job has completed
            queueJob.delete()

            # if resultCode is None, assume that job returned normally and
            # translate to a 200
            resultCode = resultCode or 200

            # if the job asked to be requeued, requeue it with a delay
            if resultCode == zerog.jobs.NO_RESULT:
                job.enqueue(delay=10)
            else:
                job.record_result(resultCode)

            return

        except (zerog.jobs.ErrorFinish, zerog.jobs.WarningFinish):
            # error has already been recorded and job is done
            queueJob.delete()
            return

        except zerog.jobs.ErrorContinue:
            # error has been recorded, but job can be tried again
            if self._manage_retries(queueJob, stats, job) is False:
                queueJob.release(delay=10)
            return

        except:
            # unknown exception occurred while job was running. Record it
            # and potentially release the job back to the queue for another
            # try
            if job:
                job.job_log_error(
                    zerog.jobs.INTERNAL_ERROR,
                    traceback.format_exc()
                )

            if self._manage_retries(queueJob, stats, job) is False:
                queueJob.release(delay=10)

    def _manage_retries(self, queueJob, stats, job):
        # Check the queue stats for a queueJob and decide if it's had enough
        # retries
        #
        # Args:
        #   queueJob: queue job object. Currently it is a beanstalkc.Job
        #   stats: dictionary of queue statistics for the queueJob
        #   job: ZeroG Job object
        #
        # Return True
        #     if the queueJob was deleted from the queue for too many reserves
        #     or too many timeouts

        # Return False
        #     if the job was not deleted
        delete = False

        if stats['reserves'] > MAX_RESERVES:
            delete = True
            tooMany = "%s reserves" % MAX_RESERVES

        elif stats['timeouts'] > MAX_TIMEOUTS:
            delete = True
            tooMany = "%s timeouts" % MAX_TIMEOUTS

        if delete:
            queueJob.delete()
            msg = "more than %s, deleting from queue" % tooMany

            if job:
                errorCode = zerog.jobs.INTERNAL_ERROR
                job.job_log_error(errorCode, msg)
                job.record_result(errorCode)
            else:
                log.error(msg)

            return True

        else:
            return False
