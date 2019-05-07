#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""

# for some godforsaken reason this has to go first otherwise
# tornado complains to all hell.
import queue

import tornado.web
import tornado.ioloop

import datastore_configs
import job_log
import registry
import worker

# import these so that the subclasses of BaseJob will be
# added by the registry.
import examples

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - "
           "%(message)s - [%(name)s:%(funcName)s]"
)
log = logging.getLogger(__name__)


class Server(tornado.web.Application):
    def __init__(self, **kwargs):
        log.info("initializing server")
        datastore_configs.set_datastore_globals()
        registry.build_registry()

        self.workers = []
        for queueName in worker.WORKER_QUEUES:
            log.info("worker listening on queue: %s" % queueName)
            self.workers.append(worker.BaseWorker(queueName))

        super(Server, self).__init__([], **kwargs)

    def set_watchdog(self, watchdog):
        log.info("setting watchdog")
        job_log.set_watchdog(watchdog)


# app = Server()


if __name__ == "__main__":
    listenPort = 8888

    log.info("geyser-service listening on port %s" % listenPort)

    application = Server()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(listenPort)
    tornado.ioloop.IOLoop.instance().start()
