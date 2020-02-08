import uuid


class CasException(Exception):
    pass


class LockedException(Exception):
    pass


class MockDatastore(object):
    """
    Mock datastore class for testing.

    Use a dictionary to simulate a key-value store
    """
    casException = CasException
    lockedException = LockedException

    def __init__(self):
        self.db = dict()

    def create(self, key, value, **kwargs):
        if key not in self.db:
            self.db[key] = dict(value=value, cas=uuid.uuid4().int)

        return True

    def read(self, key, **kwargs):
        return self.db.get(key, None)['value']

    def read_with_cas(self, key, **kwargs):
        data = self.db.get(key, None)

        if data:
            return data['value'], data['cas']
        else:
            return None, None

    def set(self, key, value, **kwargs):
        self.db[key] = dict(value=value, cas=uuid.uuid4().int)
        return True

    def set_with_cas(self, key, value, **kwargs):
        data = self.db.get(key, None)

        if data and kwargs['cas'] != data['cas']:
            raise self.casException

        newdata = dict(value=value, cas=uuid.uuid4().int)
        self.db[key] = newdata

        return True, newdata['cas']
