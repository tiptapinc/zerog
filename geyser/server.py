#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import queue
import tornado.web
import utils

import datastore_configs
import job_log
import registry

# from handlers import handlers

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - "
           "%(message)s - [%(name)s:%(funcName)s]"
)
log = logging.getLogger(__name__)


class Server(tornado.web.Application):
    def __init__(self, **kwargs):
        datastore_configs.set_datastore_globals()
        registry.build_registry()

        self.workers = []
        for queueName in queue.WORKER_QUEUES:
            log.info("worker listening on queue: %s" % queueName)
            self.workers.append(queue.BaseWorker(queueName))

        super(Server, self).__init__([], **kwargs)

    def set_watchdog(self, watchdog):
        log.info("setting watchdog")
        job_log.set_watchdog(watchdog)


app = Server()
