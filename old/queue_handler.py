#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import beanstalkc
# import json
import random
import time
import tornado.ioloop

# import geyser.utils as utils

# from . import sync_queue

import logging
log = logging.getLogger(__name__)

BEANSTALK_LOCALHOST_PORT = 11300
POLL_INTERVAL = 2


def poll_interval():
    return random.random() * POLL_INTERVAL + POLL_INTERVAL / 2.0


class QueueHandler():
    """
    base class for a consumer/producer of a beanstalk queue
    """
    def __init__(self, queueName):
        self.queueName = queueName

        # self.queue = sync_queue.QUEUE

        host = "localhost"
        port = BEANSTALK_LOCALHOST_PORT

        self.queue = beanstalkc.Connection(host=host, port=port)
        self.queue.use(self.queueName)
        self.queue.watch(self.queueName)

    def _consume(self):
        queueJob = self.queue.reserve(timeout=0)
        if queueJob is None:
            self._reconsume(time.time() + poll_interval())
        else:
            self._process_queue_job(queueJob)

    def _reconsume(self, reconsumeTime):
        tornado.ioloop.IOLoop.instance().add_timeout(
            reconsumeTime,
            self._consume
        )

    def put(self, jsonJob, **kwargs):
        response = self.queue.put(jsonJob, **kwargs)
        if isinstance(response, Exception):
            log.warning(f'queue error: {str(response)}')

    def _process_queue_job(self, queueJob):
        """
        override this method to execute the worker.
        """
        raise NotImplementedError
