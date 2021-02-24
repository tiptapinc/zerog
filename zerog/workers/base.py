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

MAX_TIMEOUTS = 2
MAX_RESERVES = 3

POLL_INTERVAL = 2


class BaseWorker(object):
    """
    Reserve jobs from a queue, instantiate those jobs, and run them.

    Run as a child process of a Server class object.

    Communicate with the parent via a multiprocessing Pipe

    Args:
        name: name of the service

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
        log.info("created worker, self: {0}".format(self))
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
        self.run_init()
        self.conn.send(json.dumps(dict(type="ready", value=True)))
        log.info(
            "starting {0} worker {1}".format(
                self.name, self.pid
            )
        )
        self.run_loop()

    def run_init(self):
        """
        sets attributes that need to be initialized in the child process
        context
        """
        self.datastore = self.makeDatastore()
        self.queue = self.makeQueue("{0}_jobs".format(self.name))
        self.pid = psutil.Process().pid
        self.draining = False

    def run_loop(self):
        """
        runs the worker's main event loop
        """
        while True:
            # check if parent has sent a message
            #
            # blocks for POLL_INTERVAL seconds, so that determines the
            # rate at which the job queue is polled
            if self.conn.poll(POLL_INTERVAL) is True:
                msg = self.conn.recv().lower()
                if msg == "drain":
                    log.info(f"worker {self.pid} start draining")
                    self.draining = True

            if not self.draining:
                # check if there is a job available in the job queue. Try
                # to run the job if so.
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
        log.info("process_queue_job, self: {0}".format(self))
        uuid = json.loads(queueJob.body)
        log.info("worker {0} reserved job {1}".format(self.pid, uuid))

        job = None
        try:
            job = self.get_job(uuid)
        except BaseException:
            msg = traceback.format_exc()
        else:
            msg = ""

        if job is None:
            # either job wasn't found or there was an exception while
            # loading it
            stats = queueJob.stats()
            msg += "worker {0} failed to load {1}, queue stats: {2}".format(
                self.pid, uuid, stats
            )
            delete = False

            if stats['reserves'] > MAX_RESERVES:
                delete = True
                tooMany = "{0} reserves".format(MAX_RESERVES)
            elif stats['timeouts'] > MAX_TIMEOUTS:
                delete = True
                tooMany = "{0} timeouts".format(MAX_TIMEOUTS)

            if delete:
                msg = "more than {0}, deleting from queue".format(tooMany)
                log.error("worker {0}, job {1}: {2}".format(
                    self.pid, uuid, msg)
                )
                queueJob.delete()
            else:
                queueJob.release(delay=30)

            return

        try:
            # run the job by calling its run method
            #
            # The run method should return a (resultCode, delay) tuple, and
            # if the resultCode == NO_RESULT, then the job is requeued with
            # the returned delay
            #
            # Not sure if it's wise to do so, but we also try to handle bad
            # return values by converting to defaults:
            #
            log.info(
                "worker {0} running job {1}".format(self.pid, uuid)
            )
            self.conn.send(
                json.dumps(dict(type="runningJobUuid", value=uuid))
            )
            job.update_attrs(running=True)
            returnVal = job.run()

            log.info(
                "worker {0} job {1}, returned {2}".format(
                    self.pid, uuid, returnVal
                )
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

        except (zerog.jobs.ErrorFinish, zerog.jobs.WarningFinish):
            # error has already been recorded and job is done
            job.record_event("Error - finished")
            queueJob.delete()
            return

        except SystemExit:
            # This will be captured and logged. Job will restart with no
            # impact on error-handling
            resultCode = zerog.jobs.NO_RESULT
            delay = 30

        except zerog.jobs.ErrorContinue:
            # Error has been recorded. Job will restart.
            job.record_event("Error - restarting")
            resultCode = job.continue_running()
            delay = 30

        except couchbase.exceptions.TimeoutError:
            # this is a temporary hack because we're seeing timeout errors
            log.error(
                "worker {0} job {1}, couchbase timeout error".format(
                    self.pid, uuid
                )
            )
            resultCode = zerog.jobs.NO_RESULT
            delay = 30

        except BaseException as e:
            # unknown exception occurred while job was running. Record it
            # and potentially release the job back to the queue for another
            # try
            msg = traceback.format_exc()
            job.record_error(zerog.jobs.INTERNAL_ERROR, msg, exception=e)
            msg += "server {0} jobType {1} job {2}".format(
                self.pid, job.JOB_TYPE, job.uuid
            )
            log.error(msg)

            resultCode = job.continue_running()
            if resultCode == zerog.jobs.NO_RESULT:
                job.record_event("Error - restarting")
                delay = 30
            else:
                job.record_event("Error - finished")

        finally:
            self.conn.send(
                json.dumps(dict(type="runningJobUuid", value=""))
            )
            job.update_attrs(running=False)

        queueJob.delete()
        if resultCode == zerog.jobs.NO_RESULT:
            job.enqueue(delay=delay)
        else:
            job.record_result(resultCode)
