#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

custom fields for use with Marshmallow

"""
from marshmallow import fields


class DictField(fields.Field):
    """
    custom field that supports dictionaries of nested fields
    with variable keys
    """
    def __init__(self, key_field, nested_field, *args, **kwargs):
        fields.Field.__init__(self, *args, **kwargs)
        self.key_field = key_field
        self.nested_field = nested_field

    def _deserialize(self, value, attr, data):
        ret = {}
        for key, val in value.items():
            k = self.key_field.deserialize(key)
            v = self.nested_field.deserialize(val)
            ret[k] = v
        return ret

    def _serialize(self, value, attr, obj):
        ret = {}
        for key, val in value.items():
            k = self.key_field._serialize(key, attr, obj)
            v = self.nested_field.serialize(
                key,
                self.get_value(attr, obj),
                accessor=lambda x, y, z: y.get(x) or z
            )
            ret[k] = v
        return ret
