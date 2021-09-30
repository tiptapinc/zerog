#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
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

            except couchbase.exceptions.TimeoutException as e:
                tries += 1
                if tries > 3:
                    raise e
                log.info(
                    "couchbase timeout in process {0} -- retrying #{1}".format(
                        psutil.Process().pid, tries
                    )
                )

    return wrapper


class CouchbaseDatastore(object):
    """
    Simple Couchbase datastore client object
    """
    casException = couchbase.exceptions.CASMismatchException
    lockedException = couchbase.exceptions.CouchbaseDataException

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

        authenticator = PasswordAuthenticator(username, password)
        self.cluster = Cluster(connectionString, ClusterOptions(authenticator))
        self.bucket = self.cluster.bucket(bucket)
        self.viewManager = self.bucket.view_indexes()
        self.collection = self.cluster.bucket(bucket).default_collection()

    @retry_on_timeouts
    def create(self, key, value, **kwargs):
        result = self.collection.insert(key, value, **kwargs)
        return result.success

    @retry_on_timeouts
    def read(self, key, **kwargs):
        kwargs['quiet'] = True
        result = self.collection.get(key, **kwargs)
        return result.content

    @retry_on_timeouts
    def read_with_cas(self, key, **kwargs):
        kwargs['quiet'] = True
        result = self.collection.get(key, **kwargs)
        return result.content, result.cas

    @retry_on_timeouts
    def lock(self, key, **kwargs):
        kwargs['quiet'] = True
        result = self.collection.get_and_lock(key, **kwargs)
        return result.content, result.cas

    @retry_on_timeouts
    def unlock(self, key, cas):
        self.collection.unlock(key, cas, quiet=True)

    @retry_on_timeouts
    def update(self, key, value, **kwargs):
        kwargs['quiet'] = True
        result = self.collection.replace(key, value, **kwargs)
        return result.success

    @retry_on_timeouts
    def update_with_cas(self, key, value, **kwargs):
        kwargs['quiet'] = True
        result = self.collection.replace(key, value, **kwargs)
        return result.success, result.cas

    @retry_on_timeouts
    def set(self, key, value, **kwargs):
        result = self.collection.upsert(key, value, **kwargs)
        return result.success

    @retry_on_timeouts
    def set_with_cas(self, key, value, **kwargs):
        # starting with Couchbase python SDK v3, upsert no longer fails
        # on incorrect CAS, so we need to create our own upsert using
        # replace
        try:
            result = self.collection.replace(key, value, **kwargs)
        except couchbase.exceptions.DocumentNotFoundException:
            result = self.collection.insert(key, value, **kwargs)
        return result.success, result.cas

    @retry_on_timeouts
    def delete(self, key, **kwargs):
        kwargs['quiet'] = True
        result = self.collection.remove(key, **kwargs)
        return result.success

    @retry_on_timeouts
    def view(self, design, view, **kwargs):
        return self.cluster.bucket.view_query(design, view, **kwargs)

    @retry_on_timeouts
    def get_multi(self, keys, **kwargs):
        return self.collection.get_multi(keys, **kwargs)

    @retry_on_timeouts
    def design_get(self, name, **kwargs):
        return self.viewManager.get_design_document(name, **kwargs)

    @retry_on_timeouts
    def design_create(self, name, ddoc, **kwargs):
        return self.bucket.viewManager.upsert_design_document(
            name, ddoc, **kwargs
        )
