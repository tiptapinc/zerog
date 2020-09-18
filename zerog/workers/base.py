#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import couchbase.exceptions
import psutil

import json
import os
import traceback

import zerog.jobs

import logging
log = logging.getLogger(__name__)

MAX_TIMEOUTS = 3
MAX_RESERVES = 3

POLL_INTERVAL = 2


class BaseWorker(object):
    """
    Reserve jobs from a queue, instantiate those jobs, and run them.

    Run as a child process of a Server class object.

    Communicate with the parent via a multiprocessing Pipe

    Args:
        makeDatastore: function to create a Datastore object that can be
                       used to persist & retrieve jobs.

        registry: JobRegistry object that maps job types to job classes

        makeQueue: function to create a Queue object to retrieve and
                   re-post jobs

        conn: connection object created by multiprocessing.Pipe()
              Used to communicate with parent.
    """
    def __init__(
        self, name, makeDatastore, makeQueue, registry, conn, **kwargs
    ):
        self.name = name
        self.makeDatastore = makeDatastore
        self.registry = registry
        self.makeQueue = makeQueue
        self.conn = conn
        self.parentPid = os.getpid()

    def get_job(self, uuid):
        return self.registry.get_job(uuid, self.datastore, self.queue, None)

    def run(self):
        """
        Process() target function.

            - Communicates with parent server.
            - Polls job queue
            - Runs jobs
        """
        self.datastore = self.makeDatastore()
        self.queue = self.makeQueue("{0}_jobs".format(self.name))
        self.pid = psutil.Process().pid
        self.runningJobs = True
        self.conn.send(json.dumps(dict(type="ready", value=True)))
        log.info(
            "starting {0} worker {1}".format(
                self.name, self.pid
            )
        )

        while True:
            # check if parent has sent a message
            #
            # blocks for POLL_INTERVAL seconds, so that determines the
            # rate at which the job queue is polled
            if self.conn.poll(POLL_INTERVAL) is True:
                msg = self.conn.recv().lower()
                if msg:
                    log.info(
                        "worker {0} received message: {1}".format(
                            self.pid, msg
                        )
                    )
                    if msg == "stop polling":
                        log.info(
                            "worker {0} stop running jobs".format(
                                self.pid
                            )
                        )
                        self.runningJobs = False

                    elif msg == "start polling":
                        log.info(
                            "worker {0} start running jobs".format(
                                self.pid
                            )
                        )
                        self.runningJobs = True

                    elif msg == "die":
                        log.info(
                            "worker {0} stopping".format(
                                self.pid
                            )
                        )
                        return

            # check if there is a job available in the job queue. Try to
            # run the job if so.
            #
            # catch DEADLINE_SOON exception here or in queue object?
            if self.runningJobs:
                queueJob = self.queue.reserve(timeout=0)
                if queueJob:
                    self._process_queue_job(queueJob)

            # check if parent is still alive. Suicide if not.
            if self._check_parent() is False:
                log.info("worker {0} orphaned".format(self.pid))
                return

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

        log.info(
            "worker {0} reserved job {1} queue stats: {2}".format(
                self.pid, uuid, stats
            )
        )
        try:
            job = None
            job = self.get_job(uuid)

            # if job failed to load, release the queueJob back to the queue
            # and return to the main loop.
            if job is None:
                log.error(
                    "worker {0} failed to load {1}. Release to queue".format(
                        self.pid, uuid
                    )
                )
                queueJob.release(delay=10)
                return

            # if job has been retried too many times, return to main loop
            # without trying to run it. The job has been deleted from the
            # queue if _manage_retries returns True
            if self._manage_retries(queueJob, stats, job):
                log.error(
                    "worker {0}, {1} excess retries. Delete from queue".format(
                        self.pid, uuid
                    )
                )
                return

            # run the job by calling its run method
            #
            # The run method should return a (resultCode, delay) tuple, and
            # if the resultCode == NO_RESULT, then the job is requeued with
            # the returned delay
            #
            # Not sure if it's wise to do so, we also try to handle bad return
            # values by converting to defaults:
            #
            self.conn.send(
                json.dumps(dict(type="runningJobUuid", value=uuid))
            )
            log.info(
                "worker {0} running job {1}".format(
                    self.pid, uuid
                )
            )

            returnVal = job.run()
            log.info(
                "worker {0} job {1}, returned {2}".format(
                    self.pid, uuid, returnVal
                )
            )
            self.conn.send(
                json.dumps(dict(type="runningJobUuid", value=""))
            )

            if isinstance(returnVal, (tuple, list)):
                # return value is a tuple, as expected, or we can accept
                # [resultCode, delay] as well
                try:
                    resultCode = int(returnVal[0])  # first resultCode
                except (ValueError, TypeError):
                    resultCode = 200
                try:
                    delay = int(returnVal[1])       # then delay
                except (ValueError, TypeError):
                    delay = 10
            else:
                # if return value is not a tuple, assume default delay
                # and assume return value is a
                delay = 10
                try:
                    resultCode = int(returnVal)
                except (ValueError, TypeError):
                    resultCode = 200

            # delete the queueJob now that the actual job has completed
            queueJob.delete()

            # if the job asked to be requeued, requeue it with a delay
            if resultCode == zerog.jobs.NO_RESULT:
                job.enqueue(delay=delay)
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

        except couchbase.exceptions.TimeoutError:
            # this is a temporary hack because we're seeing timeout errors
            log.error(
                "worker {0} job {1}, couchbase timeout error".format(
                    self.pid, uuid
                )
            )
            if job:
                job.job_log_error(
                    zerog.jobs.INTERNAL_ERROR, "couchbase timeout"
                )

            queueJob.release(delay=10)

        except:
            # unknown exception occurred while job was running. Record it
            # and potentially release the job back to the queue for another
            # try
            msg = traceback.format_exc()
            if job:
                job.job_log_error(zerog.jobs.INTERNAL_ERROR, msg)
                msg += "server {0} jobType {1} job {2}".format(
                    self.pid, job.JOB_TYPE, job.uuid
                )

            log.error(msg)

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
