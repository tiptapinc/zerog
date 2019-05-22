#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import beanstalkc
import json

import logging
log = logging.getLogger(__name__)


BEANSTALK_LOCALHOST_PORT = 11300


class SyncQueue(object):
    def __init__(self):
        # TODO: probably have to change this?
        # ports = utils.load_config("/opt/tiptap/configs/ports.yml")
        host = "localhost"
        port = BEANSTALK_LOCALHOST_PORT
        # port = ports['servicePorts']['beanstalkd']

        self.bean = beanstalkc.Connection(host=host, port=port)

    def put(self, queueName, data, **kwargs):
        self.do_bean("use", queueName)
        return self.do_bean("put", json.dumps(data), **kwargs)

    def delete(self, jobId):
        self.do_bean("delete", jobId)

    def release(self, jobId, **kwargs):
        self.do_bean("release", jobId, **kwargs)

    def touch(self, queueName, jobId):
        self.do_bean("use", queueName)
        self.do_bean("touch", jobId)

    def bury(self, jobId):
        self.do_bean("bury", jobId)

    def peek(self, jobId):
        queueJob = self.do_bean("peek", jobId)

        if queueJob:
            return queueJob.body
        else:
            return None

    def stats_job(self, jobId):
        return self.do_bean("stats_job", jobId)

    def do_bean(self, method, *args, **kwargs):
        for _ in range(3):
            try:
                return getattr(self.bean, method)(*args, **kwargs)

            except beanstalkc.SocketError:
                log.info("reconnecting to beanstalkd")
                self.__init__()

        raise beanstalkc.SocketError


QUEUE = SyncQueue()
