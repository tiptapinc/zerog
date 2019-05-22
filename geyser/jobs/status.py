#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
from marshmallow import Schema, fields, post_load

from geyser.geyser_queue import queue_globals
from geyser.utils import DictField

from .error import ErrorSchema
from .event import EventSchema


class StatusSchema(Schema):
    completenesses = DictField(
        fields.Str(),
        fields.Float(default=0)
    )
    results = DictField(
        fields.Str(),
        fields.Int(default=queue_globals.NO_RESULT)
    )
    errors = DictField(
        fields.Str(),
        fields.List(fields.Nested(ErrorSchema), default=[])
    )
    events = DictField(
        fields.Str(),
        fields.List(fields.Nested(EventSchema), default=[])
    )

    @post_load
    def make(self, data):
        return Status(**data)


class Status(object):
    def __init__(self, **kwargs):
        self.completenesses = kwargs.get('completenesses', {})
        self.results = kwargs.get('results', {})
        self.errors = kwargs.get('errors', {})
        self.events = kwargs.get('events', {})


def make_status(values):
    return StatusSchema().load(values).data
