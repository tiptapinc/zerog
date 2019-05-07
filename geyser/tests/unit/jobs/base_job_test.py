import datetime
import pytest

from geyser import jobs


def test_base_job_initializes():
    now = datetime.datetime.utcnow()

    params = {
        'documentType': 'test_document_type',
        'jobType': 'test_job_type',
        'schemaVersion': 1.0,

        'createdAt': now,
        'updatedAt': now,
        'cas': 1,

        'uuid': 'test_uuid',
        'logId': 'test_log_id',

        'queueKwargs': {},
        'queueJobId': 0,
        'events': [],
        'errors': [],
        'completeness': 0.0,
        'resultCode': -1,
        'resultString': '',
    }

    base_job = jobs.BaseJob(**params)

    assert True


def test_base_job_default_parameters():
    base_job = jobs.BaseJob()

    assert base_job.documentType == "geyser_job"
    assert base_job.jobType == "geyser_base"
    assert base_job.schemaVersion ==  1.0

    assert type(base_job.createdAt) is datetime.datetime
    assert type(base_job.updatedAt) is datetime.datetime
    assert base_job.cas == 0

    assert type(base_job.uuid) is str
    assert base_job.logId == f'{base_job.jobType}_{base_job.uuid}'

    assert base_job.queueKwargs == {}
    assert base_job.queueJobId == 0

    assert base_job.events == []
    assert base_job.errors == []

    assert base_job.completeness == 0
    assert base_job.resultCode == -1
    assert base_job.resultString == ""


def test_base_job_invalid_parameter():
    params = {
        'documentType': 10,
    }

    base_job = jobs.BaseJob(**params)
    base_job.dump()
