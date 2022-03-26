#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import datetime

from marshmallow import Schema, fields, post_load


class WarningSchema(Schema):
    """
    Schema for recording an warning that occurred while processing
    a job
    """
    timeStamp = fields.DateTime(format="iso")
    msg = fields.String(required=True)

    @post_load
    def make(self, data, **kwargs):
        return Warning(**data)


class Warning(object):
    def __init__(self, **kwargs):
        self.timeStamp = kwargs.get('timeStamp', datetime.datetime.utcnow())
        self.msg = kwargs['msg']

    def dump(self):
        return WarningSchema().dump(self)


def make_warning(msg):
    # action is required
    data = dict(msg=str(msg))
    return WarningSchema().load(data)
