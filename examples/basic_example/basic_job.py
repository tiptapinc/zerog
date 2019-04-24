from marshmallow import Schema, fields

from models import BaseJobSchema, BaseJob


class BasicSchema(BaseJobSchema):
    fieldOne = fields.String()
    fieldTwo = fields.Integer()
    fieldThree = fields.Integer()


class BasicJob(BaseJob):
    JOB_TYPE = BASIC_JOB_TYPE
    SCHEMA = BasicSchema
    QUEUE_NAME = 'basic_job'

    def __init__(self, **kwargs):
        super(BasicJob, self).__init__(**kwargs)

        self.fieldOne = kwargs.get('fieldOne')
        self.fieldTwo = kwargs.get('fieldTwo')
        self.fieldThree = kwargs.get('fieldThree')

    def run(self):
        # Adds the contents of fieldTwo and fieldThree
        pass
