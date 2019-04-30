#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2016 MotiveMetrics. All rights reserved.

module-wide global

"""
import datastore

DATASTORE = None

LOCALHOST_COUCHBASE_PORT = 8091
BUCKET_NAME = "geyser"


def set_datastore_globals():
    global DATASTORE

    port = LOCALHOST_COUCHBASE_PORT
    bucket = BUCKET_NAME

    if DATASTORE is None:
        DATASTORE = datastore.Datastore(
            "localhost",
            port,
            bucket,
            password="password",
        )
