#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import json
import tornado.escape
from tornado.web import HTTPError

from .base import BaseHandler

import logging
log = logging.getLogger(__name__)

JOB_TYPE_PATT = "(?P<jobtype>[^/]+)"


class RunJobHandler(BaseHandler):
    def post(self, *args, **kwargs):
        """
            Args:
                jobType: must be extractable from the request by the
                         derive_job_type method
        """
        try:
            data = tornado.escape.json_decode(self.request.body)
        except:
            data = {}

        jobType = self.derive_job_type(data, *args, **kwargs)
        log.info(
            "creating ZeroG Job of type:%s, from data\n%s" %
            (jobType, json.dumps(data, indent=4))
        )
        job = self.application.make_job(data, jobType)

        if job:
            job.enqueue()
            self.complete(
                201, output=json.dumps(dict(uuid=job.uuid), indent=4)
            )
        else:
            raise HTTPError(
                400,
                (
                    "Could not create job of type:%s from data\n%s" %
                    (jobType, json.dumps(data, indent=4))
                )
            )

    def derive_job_type(self, data, *args, **kwargs):
        """
        Extract jobType from the POST. Handles several different options:

            - named keyword argument extracted from the URL
            - positional argument extracted from the URL
            - field in a json-encoded request body

        Override this method to custom-synthesize jobType from the request
        """
        if "jobtype" in kwargs:
            return kwargs['jobtype']
        elif len(args) >= 1:
            return args[0]
        elif "jobType" in data:
            return data['jobType']
        else:
            raise HTTPError(400, "Could not extract jobType from request")
