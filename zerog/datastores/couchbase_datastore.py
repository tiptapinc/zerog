#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.exceptions import KeyExistsError, TemporaryFailError
import couchbase.exceptions
import psutil

import logging
log = logging.getLogger(__name__)

CONNECTION_KWARGS = [
    # this list is incomplete
    "operation_timeout",
    "config_total_timeout",
    "config_node_timeout"
]


def retry_on_timeouts(func):
    def wrapper(*args, **kwargs):
        tries = 0
        while True:
            try:
                return func(*args, **kwargs)
            except couchbase.exceptions.TimeoutError:
                pass

            tries += 1
            if tries == 3:
                raise couchbase.exceptions.TimeoutError

            log.info(
                "couchbase timeout in process {0} -- retrying".format(
                    psutil.Process().pid
                )
            )
    return wrapper


class CouchbaseDatastore(object):
    """
    Simple Couchbase datastore client object
    """
    casException = KeyExistsError
    lockedException = TemporaryFailError

    def __init__(self, host, username, password, bucket, **kwargs):
        connectionString = "couchbase://{0}".format(host)

        connectArgs = []
        for arg in CONNECTION_KWARGS:
            value = kwargs.pop(arg, None)
            if value:
                connectArgs.append("{0}={1}".format(arg, value))

        queryStr = "&".join(connectArgs)
        if queryStr:
            connectionString += "/?{0}".format(queryStr)

        cluster = Cluster(connectionString)
        authenticator = PasswordAuthenticator(username, password)
        cluster.authenticate(authenticator)
        kwargs['quiet'] = True
        self.bucket = cluster.open_bucket(bucket, **kwargs)

    @retry_on_timeouts
    def create(self, key, value, **kwargs):
        ro = self.bucket.insert(key, value, **kwargs)
        return ro.success

    @retry_on_timeouts
    def read(self, key, **kwargs):
        rv = self.bucket.get(key, **kwargs)
        return rv.value

    @retry_on_timeouts
    def read_with_cas(self, key, **kwargs):
        rv = self.bucket.get(key, **kwargs)
        return rv.value, rv.cas

    @retry_on_timeouts
    def lock(self, key, **kwargs):
        rv = self.bucket.lock(key, **kwargs)
        return rv.value, rv.cas

    @retry_on_timeouts
    def unlock(self, key, cas):
        self.bucket.unlock(key, cas)

    @retry_on_timeouts
    def update(self, key, value, **kwargs):
        ro = self.bucket.replace(key, value, **kwargs)
        return ro.success

    @retry_on_timeouts
    def update_with_cas(self, key, value, **kwargs):
        ro = self.bucket.replace(key, value, **kwargs)
        return ro.success, ro.cas

    @retry_on_timeouts
    def set(self, key, value, **kwargs):
        ro = self.bucket.upsert(key, value, **kwargs)
        return ro.success

    @retry_on_timeouts
    def set_with_cas(self, key, value, **kwargs):
        ro = self.bucket.upsert(key, value, **kwargs)
        return ro.success, ro.cas

    @retry_on_timeouts
    def delete(self, key, **kwargs):
        ro = self.bucket.remove(key, **kwargs)
        return ro.success

    @retry_on_timeouts
    def view(self, design, view, **kwargs):
        return self.bucket.query(design, view, **kwargs)

    @retry_on_timeouts
    def get_multi(self, keys, **kwargs):
        return self.bucket.get_multi(keys, **kwargs)

    @retry_on_timeouts
    def design_get(self, name, **kwargs):
        return self.bucket.bucket_manager().design_get(name, **kwargs)

    @retry_on_timeouts
    def design_create(self, name, ddoc, **kwargs):
        return self.bucket.bucket_manager().design_create(name, ddoc, **kwargs)
