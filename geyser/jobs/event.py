#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import datetime

from marshmallow import Schema, fields, post_load


class EventSchema(Schema):
    """
    Schema for recording an event that occurred while processing
    a job
    """
    timeStamp = fields.DateTime(format="iso")
    action = fields.String()
    msg = fields.String()

    @post_load
    def make(self, data):
        return Event(**data)


class Event(object):
    def __init__(self, **kwargs):
        self.timeStamp = kwargs.get('timeStamp', datetime.datetime.utcnow())

        # only create non-null attributes that are in kwargs
        for attr in ['action', 'msg']:
            if attr in kwargs and kwargs[attr]:
                setattr(self, attr, kwargs[attr])

    def dump(self):
        return EventSchema().dump(self).data


def make_event(action, msg=None):
    # action is required
    values = dict(action=action, msg=msg)
    return EventSchema(strict=True).load(values).data
