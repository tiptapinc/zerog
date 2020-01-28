#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import json
import tornado.web

import logging
log = logging.getLogger(__name__)


class BaseHandler(tornado.web.RequestHandler):
    def __init__(self, *args, **kwargs):
        super(BaseHandler, self).__init__(*args, **kwargs)
        self.registry = self.application.registry

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header(
            "Access-Control-Allow-Headers",
            "content-type, x-requested-with"
        )
        self.set_header(
            'Access-Control-Allow-Methods',
            'GET,POST,PUT,DELETE,OPTIONS'
        )

    def options(self, *args, **kwargs):
        # no body
        self.set_status(204)
        self.finish()

    def write_error(self, status_code, **kwargs):
        if 'exc_info' in kwargs:
            etype, value, traceback = kwargs['exc_info']

            if etype == tornado.web.HTTPError and value.log_message:
                self.write(value.log_message)

            # do something here with marshmallow validation errors
            # elif etype == webargs.tornadoparser.HTTPError:
            #     self.set_header("content-type", "application/json")
            #     self.write(json.dumps(value.messages, indent=4))

            else:
                errDict = dict(errorType=etype.__name__, errorValue=str(value))
                self.set_header("content-type", "application/json")
                self.write(json.dumps(errDict, indent=4))

        self.finish()

    def _complete(self, statusCode, **kwargs):
        if kwargs.get('errMsg'):
            log.warning(kwargs['errMsg'])
            self.write("%s\n" % kwargs['errMsg'])

        if 'file' in kwargs:
            self.set_header("content-type", "application/octet-stream")
            if 'fileName' in kwargs:
                self.set_header(
                    "content-disposition",
                    "attachment;filename=%s" % kwargs['fileName']
                )
            self.write(kwargs['file'])

        elif 'output' in kwargs:
            self.set_header("content-type", "application/json")
            self.write("%s\n" % kwargs['output'])

        self.set_status(statusCode)
        self.finish()
