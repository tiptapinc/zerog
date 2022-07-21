#!/usr/bin/env python
# encoding: utf-8
# Copyright (c) 2017-2021 MotiveMetrics. All rights reserved.
"""
ZeroG ErrorSchema and Error class definitions
"""
import datetime

from marshmallow import Schema, fields, post_load


class ErrorSchema(Schema):
    """
    Schema for recording an error that occurred while processing
    a job
    """
    timeStamp = fields.DateTime(format="iso")
    errorCode = fields.Integer(required=True)
    msg = fields.String(required=True)

    @post_load
    def make(self, data, **kwargs):
        return Error(**data)


class Error(object):
    """
    Error object used to record errors in a ZeroG BaseJob
    """
    def __init__(self, **kwargs):
        self.timeStamp = kwargs.get('timeStamp', datetime.datetime.utcnow())
        self.errorCode = kwargs['errorCode']
        self.msg = kwargs['msg']

    def dump(self):
        return ErrorSchema().dump(self)


def make_error(errorCode, msg):
    # action is required
    data = dict(errorCode=errorCode, msg=str(msg))
    return ErrorSchema().load(data)
