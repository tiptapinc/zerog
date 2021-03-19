#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2021 MotiveMetrics. All rights reserved.

"""
DELIM = "+$"


def make_worker_id(workerType, host, serviceName, pid):
    return f"{workerType}{DELIM}{host}{DELIM}{serviceName}{DELIM}{pid}"


def parse_worker_id(workerId):
    split = workerId.split(DELIM)
    if len(split) == 4:
        return dict(
            workerType=split[0],
            host=split[1],
            serviceName=split[2],
            pid=int(split[3])
        )
    else:
        return None
