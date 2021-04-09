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
        self.make_connection()

    def make_connection(self, retries=0):
        while True:
            try:
                self.bean = beanstalkc.Connection(
                    host=self.host, port=self.port
                )
                self.attach()
                return

            except beanstalkc.SocketError:
                pass

            if retries == 0:
                break

            retries -= 1
            time.sleep(5)

        raise beanstalkc.SocketError

    def put(self, data, **kwargs):
        return self.do_bean("put", json.dumps(data), **kwargs)

    def reserve(self, **kwargs):
        return self.do_bean("reserve", **kwargs)

    def attach(self):
        self.do_bean("use", self.queueName)
        self.do_bean("watch", self.queueName)

    def detach(self):
        self.do_bean("use", "default")
        self.do_bean("ignore", self.queueName)

    def delete(self, jid):
        self.do_bean("delete", jid)

    def list_all_queues(self):
        return self.do_bean("tubes")

    def do_bean(self, method, *args, **kwargs):
        try:
            return getattr(self.bean, method)(*args, **kwargs)

        except beanstalkc.SocketError:
            self.make_connection(3)


class QueueJob(beanstalkc.Job):
    """
    this is just for testing
    """
    def __init__(self, queue, body, jid=1):
        super(QueueJob, self).__init__(queue.bean, jid, body)
