#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import queue
import tornado.web
import tornado.ioloop

import datastore_configs
import registry

from examples import handlers

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - "
           "%(message)s - [%(name)s:%(funcName)s]"
)
log = logging.getLogger(__name__)


class Server(tornado.web.Application):
    def __init__(self, **kwargs):
        log.info("initializing example server")
        registry.build_registry()
        datastore_configs.set_datastore_globals()

        super(Server, self).__init__(handlers, **kwargs)


# app = Server()


if __name__ == "__main__":
    listenPort = 8880

    log.info("geyser-api listening on port %s" % listenPort)

    application = Server()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(listenPort)
    tornado.ioloop.IOLoop.instance().start()
