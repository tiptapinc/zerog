#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

Simple beanstalkd client
"""
import beanstalkc
import json

import logging
log = logging.getLogger(__name__)


class BeanstalkdQueue(object):
    def __init__(self, host, port, queueName):
        self.host = host
        self.port = port
        self.bean = beanstalkc.Connection(host=host, port=port)
        self.do_bean("use", queueName)
        self.do_bean("watch", queueName)

    def put(self, data, **kwargs):
        return self.do_bean("put", json.dumps(data), **kwargs)

    def reserve(self, **kwargs):
        return self.do_bean("reserve", **kwargs)

    def peek(self, jobId):
        queueJob = self.do_bean("peek", jobId)

        if queueJob:
            return queueJob.body
        else:
            return None

    def do_bean(self, method, *args, **kwargs):
        for _ in range(3):
            try:
                return getattr(self.bean, method)(*args, **kwargs)

            except beanstalkc.SocketError:
                log.warning("lost connection to beanstalkd - reconnecting")
                self.__init__()

        raise beanstalkc.SocketError
