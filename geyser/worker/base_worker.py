#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import beanstalkt
import json
import random
import time
import traceback

import job_log

from queue import queue_globals
from queue import sync_queue
from queue import work_queue

from jobs import get_base_job

import logging
log = logging.getLogger(__name__)

MAX_TIMEOUTS = 3
MAX_RESERVES = 3

POLL_INTERVAL = 2


def poll_interval():
    return random.random() * POLL_INTERVAL + POLL_INTERVAL / 2.0


class BaseWorker(work_queue.PollHandler):
    """
    Base worker class, which consumes jobs from a queue
    and runs them.

    Handles uncaught exceptions and ensures they are recorded.

    Jobs MUST be a subclass of base_job.BaseJob

    """
    def __init__(self, queueName):
        super(BaseWorker, self).__init__(queueName)

        self.queueName = queueName

        # defined in work_queue.PollHandler, reserves a job from beanstalk with
        # callback _process_queue_job
        self._consume()

    def _process_queue_job(self, queueJob):
        self.consuming = False

        # TimedOut is from beanstalk, restart queue consumption in
        # poll_interval() time
        if isinstance(queueJob, beanstalkt.TimedOut):
            self._reconsume(time.time() + poll_interval())
            return

        # if the job is an Exception of any kind, log it and restart queue
        # consumption in poll_interval() time
        elif isinstance(queueJob, Exception):
            log.warning(
                "exception for queue %s: %s" %
                (self.queueName, str(queueJob))
            )
            self._reconsume(time.time() + poll_interval())
            return

        # queueJobId = id given to job by beanstalk
        queueJobId = queueJob['id']
        stats = sync_queue.QUEUE.stats_job(queueJobId)

        # jobId = uuid used to identify job in database
        jobId = json.loads(queueJob['body'])

        try:
            job = None
            job = get_base_job(jobId)

            # job has been retried too many times, restart queue consumption
            # in poll_interval() time
            if self.manage_retries(queueJobId, stats, jobId, job):
                self._reconsume(time.time() + poll_interval())
                return

            # job doesn't exist, restart queue consumption in poll_interval()
            # time
            if job is None:
                log.error("could not load job: %s" % jobId)
                self.queue.release(queueJobId, delay=10)
                self._reconsume(time.time() + poll_interval())
                return

            # set CURRENT_JOB to be job
            job_log.set_job(job, self)

            # run the job
            resumeAt, requeue = job.run()

            # set CURRENT_JOB to None
            job_log.unset_job()

            # delete the job now that it has run
            self.queue.delete(queueJobId)

            # requeue job after a delay
            if requeue:
                delay = max(0, resumeAt - time.time())
                job.enqueue(delay=delay)

            # restart queue consumption in poll_interval() time
            self._reconsume(time.time() + poll_interval())

        except queue_globals.WFErrorFinish:
            # error has already been recorded and job is done
            self.queue.delete(queueJobId)
            self._reconsume(time.time() + poll_interval())
            return

        except queue_globals.WFErrorContinue:
            # error has been recorded, but job can be tried again
            if self.manage_retries(queueJobId, stats, jobId, job) is False:
                self.queue.release(queueJobId, delay=10)

            self._reconsume(time.time() + poll_interval())
            return

        except:
            if job:
                job.record_error(
                    queue_globals.INTERNAL_ERROR,
                    traceback.format_exc()
                )

            if self.manage_retries(queueJobId, stats, jobId, job) is False:
                self.queue.release(queueJobId, delay=10)

            self._reconsume(time.time() + poll_interval())
            raise

    def manage_retries(self, queueJobId, stats, jobId, job):
        """
        Return True
            if the job was deleted from the queue for too many reserves
            or too many timeouts

        Return False
            if the job was not deleted
        """
        delete = False

        if stats['reserves'] > MAX_RESERVES:
            delete = True
            tooMany = "%s reserves" % MAX_RESERVES

        elif stats['timeouts'] > MAX_TIMEOUTS:
            delete = True
            tooMany = "%s timeouts" % MAX_TIMEOUTS

        if delete:
            self.queue.delete(queueJobId)
            msg = "more than %s, deleting from queue" % tooMany

            if job:
                job.record_result(queue_globals.INTERNAL_ERROR, msg)
            else:
                log.warning("%s: %s" % (jobId, msg))

            return True
        else:
            return False


def register_worker(model):
    if model.QUEUE_NAME:
        WORKER_QUEUES.append(model.QUEUE_NAME)


WORKER_QUEUES = []
