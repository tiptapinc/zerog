## SNIPPET 1
# Create a basic schema and job that uses that schema.
# Has nothing to do with database things.

import datetime
import random
import uuid

from marshmallow import Schema, fields

class DatabaseUpdateException(Exception):
    pass

class MySchema(Schema):
    fieldOne = fields.String()
    fieldTwo = fields.Integer()
    fieldThree = fields.Dict()
    uuid = fields.String()
    createdAt = fields.DateTime(format="iso")
    updatedAt = fields.DateTime(format="iso")


class MyJob(object):
    FIELD_ONE = 'field one'
    FIELD_TWO = 2
    FIELD_THREE = { 'field': 3 }
    SCHEMA = MySchema

    def __init__(self, **kwargs):
        now = datetime.datetime.utcnow()
        self.fieldOne = kwargs.get('fieldOne', self.FIELD_ONE)
        self.fieldTwo = kwargs.get('fieldTwo', self.FIELD_TWO)
        self.fieldThree = kwargs.get('fieldThree', self.FIELD_THREE)
        self.uuid = (kwargs.get('uuid') or str(uuid.uuid4()))
        self.createdAt = kwargs.get('createdAt', now)
        self.updatedAt = kwargs.get('updatedAt', now)

    def dump(self):
        return self.SCHEMA().dump(self).data

    def dumps(self, **kwargs):
        return self.SCHEMA().dumps(self, **kwargs).data

    def __str__(self):
        return self.dumps(indent=4)

    def key(self):
        return "%s_%s" % ("TEST", self.uuid)

    def save(self):
        self.updatedAt = datetime.datetime.utcnow()

        # make it randomly fail??
        # mimic database optimistic locking
        if random.random() < 0.25:
            raise DatabaseUpdateException

    def reload(self):
        print("reloaded")

    def record_change(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
            self.save()
            return True

        except DatabaseUpdateException:
            print("database update error - reloading")

        print("save failed - too many tries")
        return False

    def update_attrs(self, **kwargs):
        def do_update_attrs():
            for attr, value in kwargs.items():
                setattr(self, attr, value)

        print("updating...")
        self.record_change(do_update_attrs)


## SNIPPET 2
# Possibly determine if record_change rolls back attribute changes

def make_and_update_job(n):
    job = MyJob()
    print(f'--- Job {n} ---')
    print(job)

    job.update_attrs(fieldOne=f'job{n}')
    print(job)


def main():
    for n in range(5):
        print()
        make_and_update_job(n)


if __name__ == '__main__':
    main()
