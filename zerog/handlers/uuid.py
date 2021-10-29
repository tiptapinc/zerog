#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
from abc import abstractmethod
import json
from tornado.web import HTTPError

from .base import BaseHandler

import logging
log = logging.getLogger(__name__)

UUID_PATT = "(?P<uuid>[^/]+)"           # kwarg == "uuid"


class UuidHandler(BaseHandler):
    def get(self, *args, **kwargs):
        """
            Args:
                uuid: must be extractable from the request by the
                      derive_uuid method
        """
        uuid = self.derive_uuid(*args, **kwargs)
        job = self.application.get_job(uuid)

        if job:
            self.do_get(job)
        else:
            raise HTTPError(404, "Could not find job %s" % uuid)

    @abstractmethod
    def do_get(self, job):
        pass

    def derive_uuid(self, *args, **kwargs):
        """
        Extract uuid from the GET. Handles several different options:

            - named keyword argument extracted from the URL
            - positional argument extracted from the URL
            - query argument in the query string

        Override this method to custom-synthesize jobType from the request
        """
        if "uuid" in kwargs:
            return kwargs['uuid']
        elif len(args) >= 1:
            return args[0]
        else:
            uuid = self.get_query_argument("uuid", None)

            if uuid:
                return uuid
            else:
                raise HTTPError(
                    400, "{0}.get needs 'uuid' argument".format(
                        self.__class__.__name__
                    )
                )


class ProgressHandler(UuidHandler):
    def do_get(self, job):
        self.complete(200, output=json.dumps(
            job.progress(), indent=4, allow_nan=False)
        )


class GetDataHandler(UuidHandler):
    def do_get(self, job):
        self.complete(200, output=json.dumps(
            job.get_data(), indent=4, allow_nan=False)
        )


class InfoHandler(UuidHandler):
    def do_get(self, job):
        self.complete(200, output=json.dumps(
            job.info(), indent=4, allow_nan=False)
        )


class DumpHandler(UuidHandler):
    def do_get(self, job):
        self.complete(200, output=job.dumps(indent=4, allow_nan=False))
