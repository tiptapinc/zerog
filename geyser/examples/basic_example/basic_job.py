import time

import job_log

from marshmallow import fields

from jobs import BaseJobSchema, BaseJob, make_base_job

BASIC_JOB_TYPE = "basic_job"


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
        job_log.info('Starting processing basic job')
        time.sleep(5)

        job_log.info('Finished processing basic job')
        self.record_result(200)

        return time.time(), False


def make_basic_job(values={}):
    return make_base_job(values, BASIC_JOB_TYPE)
