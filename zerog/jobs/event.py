#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import datetime

from marshmallow import Schema, fields, post_load


class EventSchema(Schema):
    """
    Schema for recording an event that occurred while processing a job
    """
    timeStamp = fields.DateTime(format="iso")
    msg = fields.String(required=True)

    @post_load
    def make(self, data, **kwargs):
        return Event(**data)


class Event(object):
    def __init__(self, **kwargs):
        self.timeStamp = kwargs.get('timeStamp', datetime.datetime.utcnow())
        self.msg = kwargs['msg']

    def dump(self):
        return EventSchema().dump(self)


def make_event(msg):
    # action is required
    data = dict(msg=str(msg))
    return EventSchema().load(data)
