#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

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
    msg = fields.String()

    @post_load
    def make(self, data):
        return Error(**data)


class Error(object):
    def __init__(self, **kwargs):
        self.timeStamp = kwargs.get('timeStamp', datetime.datetime.utcnow())

        # only create non-null attributes that are in kwargs
        for attr in ['errorCode', 'msg']:
            if attr in kwargs and kwargs[attr]:
                setattr(self, attr, kwargs[attr])

    def dump(self):
        return ErrorSchema().dump(self).data


def make_error(errorCode, msg=None):
    # action is required
    values = dict(errorCode=errorCode, msg=msg)
    return ErrorSchema(strict=True).load(values).data
