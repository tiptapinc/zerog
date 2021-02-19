#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2021 MotiveMetrics. All rights reserved.

"""


def make_worker_id(workerType, thisHost, serviceName, pid):
    delim = "+$"
    return f"{workerType}{delim}{thisHost}{delim}{serviceName}{delim}{pid}"
