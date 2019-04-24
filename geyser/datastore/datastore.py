from couchbase.bucket import Bucket

# ignore linter! do not delete!
from couchbase.exceptions import NotFoundError

import logging
log = logging.getLogger(__name__)


class Datastore(object):
    """
    This class provides helper functions to interface with the database
    that stores job information.

    Currently database being used is Couchbase.
    """

    def __init__(self, host, port, bucket, **kwargs):
        connectionString = "http://%s:%s/%s" % (host, port, bucket)
        kwargs['quiet'] = True
        self.bucket = Bucket(connectionString, **kwargs)

    def create(self, key, value, **kwargs):
        ro = self.bucket.insert(key, value, **kwargs)
        return ro.success

    def read(self, key, **kwargs):
        rv = self.bucket.get(key, **kwargs)
        return rv.value

    def read_with_cas(self, key, **kwargs):
        rv = self.bucket.get(key, **kwargs)
        return rv.value, rv.cas

    def lock(self, key, **kwargs):
        rv = self.bucket.lock(key, **kwargs)
        return rv.value, rv.cas

    def unlock(self, key, cas):
        self.bucket.unlock(key, cas)

    def update(self, key, value, **kwargs):
        ro = self.bucket.replace(key, value, **kwargs)
        return ro.success

    def update_with_cas(self, key, value, **kwargs):
        ro = self.bucket.replace(key, value, **kwargs)
        return ro.success, ro.cas

    def set(self, key, value, **kwargs):
        ro = self.bucket.upsert(key, value, **kwargs)
        return ro.success

    def set_with_cas(self, key, value, **kwargs):
        ro = self.bucket.upsert(key, value, **kwargs)
        return ro.success, ro.cas

    def delete(self, key, **kwargs):
        ro = self.bucket.remove(key, **kwargs)
        return ro.success

    def view(self, design, view, **kwargs):
        return self.bucket.query(design, view, **kwargs)

    def get_multi(self, keys, **kwargs):
        return self.bucket.get_multi(keys, **kwargs)

    def design_get(self, name, **kwargs):
        return self.bucket.bucket_manager().design_get(name, **kwargs)

    def design_create(self, name, ddoc, **kwargs):
        return self.bucket.bucket_manager().design_create(name, ddoc, **kwargs)
