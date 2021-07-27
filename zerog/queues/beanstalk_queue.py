#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

Simple beanstalkd client
"""
import beanstalkc
import json
import time

import logging
log = logging.getLogger(__name__)


class BeanstalkdQueue(object):
    def __init__(self, host, port, queueName):
        self.host = host
        self.port = port
        self.queueName = queueName

        for _ in range(3):
            try:
                self.make_connection()
                self.attach()

            except beanstalkc.SocketError:
                pass

            time.sleep(2)

        # we can only get here after a beanstalkc.SocketError
        log.warning("failed to connect to beanstalkd queue")
        raise beanstalkc.SocketError

    def make_connection(self):
        self.bean = beanstalkc.Connection(
            host=self.host, port=self.port
        )

    def put(self, data, **kwargs):
        return self.do_bean("put", json.dumps(data), **kwargs)

    def reserve(self, **kwargs):
        return self.do_bean("reserve", **kwargs)

    def attach(self):
        self.do_bean("ignore", "default")
        self.do_bean("use", self.queueName)
        self.do_bean("watch", self.queueName)

    def detach(self):
        self.do_bean("use", "default")
        self.do_bean("ignore", self.queueName)

    def delete(self, jid):
        self.do_bean("delete", jid)

    def release(self, jid, **kwargs):
        self.do_bean("release", jid, **kwargs)

    def stats_job(self, jid):
        self.do_bean("stats_job", jid)

    def list_all_queues(self):
        return self.do_bean("tubes")

    def do_bean(self, method, *args, **kwargs):
        # try to execute the method.
        #   - if it succeeds, return the result
        #   - if there's a socket error, fall through to the retry logic
        #   - any other exception is not caught
        try:
            return getattr(self.bean, method)(*args, **kwargs)

        except beanstalkc.SocketError:
            pass

        # initial attempt to execute the method failed, but we may be
        # able to re-establish the beanstalkd connection and then execute
        # successfully
        log.info("reconnecting to beanstalkd queue")
        for _ in range(2):
            try:
                self.make_connection()
                self.attach()
                result = getattr(self.bean, method)(*args, **kwargs)
                log.info("successfully reconnected")
                return result

            except beanstalkc.SocketError:
                pass

            time.sleep(1)

        # we can only get here after a beanstalkc.SocketError
        log.warning("failed to reconnect to beanstalkd queue")


class QueueJob(beanstalkc.Job):
    """
    this is just for testing
    """
    def __init__(self, queue, body, jid=1):
        super(QueueJob, self).__init__(queue.bean, jid, body)
