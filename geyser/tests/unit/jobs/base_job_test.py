import datetime
import json
import marshmallow
import pytest

# pytest debugger
# use pdb.set_trace() to walk through a test
import pdb

from geyser.datastore import KeyExistsError, TemporaryFailError
from geyser import datastore_configs
from geyser import jobs

#
# Unit tests for base_job.
#
# Notes:
#   - make_key in BaseJob.key called with self.DOCUMENT_TYPE instead of
#     self.documentType
#   - mocking the global datastore_config.DATASTORE is super hard => better way
#     of configuring which datastore to use?
#   - BaseJob.record_change will update the attributes on the base job instance
#     even if the save to the datastore fails
#   - test_record_error_with_event_message => BaseJob.record_error with an
#     eventMsg creates an event with action = eventMsg instead of
#     msg = eventMsg which is inconsistent with BaseJob.record_event
#   - added BaseJob._validate_parameters
#
# todo (mslaughter):
#   - figure out behavior for skipped tests
#   - refactor repeated mocking for base_job.save
#


class TestBaseJob(object):
    def test_initializes(self):
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

        jobs.BaseJob(**params)

        assert True

    def test_with_default_parameters(self):
        base_job = jobs.BaseJob()

        assert base_job.documentType == "geyser_job"
        assert base_job.jobType == "geyser_base"
        assert base_job.schemaVersion == 1.0

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

    def test_with_invalid_parameter(self):
        params = {
            'schemaVersion': 'test',
        }

        with pytest.raises(marshmallow.exceptions.ValidationError):
            jobs.BaseJob(**params)

        params2 = {
            'documentType': 20,
        }

        with pytest.raises(marshmallow.exceptions.ValidationError):
            jobs.BaseJob(**params2)

    def test_dump(self):
        params = {
            'documentType': 'test_document_type',
            'jobType': 'test_job_type',
            'schemaVersion': 1.0,

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

        base_job_dump = base_job.dump()
        assert type(base_job_dump) is dict

        assert base_job_dump['documentType'] == params['documentType']
        assert base_job_dump['jobType'] == params['jobType']
        assert base_job_dump['schemaVersion'] == params['schemaVersion']

        assert base_job_dump['cas'] == params['cas']

        assert base_job_dump['uuid'] == params['uuid']
        assert base_job_dump['logId'] == params['logId']

        assert base_job_dump['queueKwargs'] == params['queueKwargs']
        assert base_job_dump['queueJobId'] == params['queueJobId']

        assert base_job_dump['events'] == params['events']
        assert base_job_dump['errors'] == params['errors']

        assert base_job_dump['completeness'] == params['completeness']
        assert base_job_dump['resultCode'] == params['resultCode']
        assert base_job_dump['resultString'] == params['resultString']

    def test_dumps(self):
        params = {
            'documentType': 'test_document_type',
            'jobType': 'test_job_type',
            'schemaVersion': 1.0,

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

        base_job_dumps = base_job.dumps()
        assert type(base_job_dumps) is str

        base_job_json = json.loads(base_job_dumps)
        assert base_job_json['documentType'] == params['documentType']
        assert base_job_json['jobType'] == params['jobType']
        assert base_job_json['schemaVersion'] == params['schemaVersion']

        assert base_job_json['cas'] == params['cas']

        assert base_job_json['uuid'] == params['uuid']
        assert base_job_json['logId'] == params['logId']

        assert base_job_json['queueKwargs'] == params['queueKwargs']
        assert base_job_json['queueJobId'] == params['queueJobId']

        assert base_job_json['events'] == params['events']
        assert base_job_json['errors'] == params['errors']

        assert base_job_json['completeness'] == params['completeness']
        assert base_job_json['resultCode'] == params['resultCode']
        assert base_job_json['resultString'] == params['resultString']

    @pytest.mark.skip(reason="not sure desired outcome here")
    def test_key(self):
        params = {
            'documentType': 'test_document_type',
            'uuid': 'test_uuid',

        }
        base_job = jobs.BaseJob(**params)
        assert base_job.key() == f'{params["documentType"]}_{params["uuid"]}'

    @pytest.mark.skip(reason="super hard to mock while DATASTORE is global")
    def test_save(self, setup_database_globals, mocker):
        mocker.patch.object(datastore_configs.DATASTORE, 'set_with_cas')
        datastore_configs.DATASTORE.set_with_cas.return_value = {}, 10

        base_job = jobs.BaseJob()

        # pdb.set_trace()

        base_job.save()

    def test_record_change_save_successful(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.return_value = None

        def update_func(*args, **kwargs):
            for attr, value in kwargs.items():
                setattr(base_job, attr, value)

        update_params = {
            'schemaVersion': 2.0,
        }
        outcome = base_job.record_change(update_func, **update_params)
        base_job.save.assert_called_once_with()

        assert outcome is True
        assert base_job.schemaVersion == update_params['schemaVersion']

    def test_record_change_save_successful_with_retries(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.side_effect = [
            KeyExistsError('key does not exist'),
            TemporaryFailError('temporary error'),
            None,
        ]

        mocker.patch.object(base_job, 'reload')
        base_job.reload.return_value = None

        def update_func(*args, **kwargs):
            for attr, value in kwargs.items():
                setattr(base_job, attr, value)

        update_params = {
            'schemaVersion': 2.0,
        }
        outcome = base_job.record_change(update_func, **update_params)
        assert base_job.save.call_count == 3

        assert outcome is True

    def test_record_change_save_unsuccessful_retries_10_times(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.side_effect = KeyExistsError('key does not exist')

        mocker.patch.object(base_job, 'reload')
        base_job.reload.return_value = None

        def update_func(*args, **kwargs):
            for attr, value in kwargs.items():
                setattr(base_job, attr, value)

        update_params = {
            'schemaVersion': 2.0,
        }
        outcome = base_job.record_change(update_func, **update_params)
        assert base_job.save.call_count == 10

        assert outcome is False

    @pytest.mark.skip(reason="not sure what the desired outcome is here")
    def test_record_change_save_unsuccessful_attrs_do_not_update(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.side_effect = KeyExistsError('key does not exist')

        mocker.patch.object(base_job, 'reload')
        base_job.reload.return_value = None

        def update_func(*args, **kwargs):
            for attr, value in kwargs.items():
                setattr(base_job, attr, value)

        update_params = {
            'schemaVersion': 2.0,
        }
        outcome = base_job.record_change(update_func, **update_params)
        assert base_job.save.call_count == 10

        assert outcome is False
        assert base_job.schemaVersion == 1.0

    def test_update_attrs(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.return_value = None

        update_params = {
            'schemaVersion': 2.0,
        }
        base_job.update_attrs(**update_params)
        base_job.save.assert_called_once_with()

        assert base_job.schemaVersion == update_params['schemaVersion']

    def test_record_event(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.return_value = None

        msg = 'test-event'
        base_job.record_event(msg)

        base_job.save.assert_called_once_with()

        assert len(base_job.events) == 1
        assert base_job.events[0].msg == 'test-event'

    def test_record_error(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.return_value = None

        errorCode = 500
        msg = 'test-error'
        base_job.record_error(errorCode, msg)

        base_job.save.assert_called_once_with()

        assert len(base_job.errors) == 1
        assert base_job.errors[0].errorCode == 500
        assert base_job.errors[0].msg == 'test-error'

    def test_record_error_with_event_message(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.return_value = None

        errorCode = 500
        msg = 'test-error'
        eventMsg = 'test-event'
        base_job.record_error(errorCode, msg, eventMsg)

        base_job.save.assert_called_once_with()

        assert len(base_job.errors) == 1
        assert base_job.errors[0].errorCode == 500
        assert base_job.errors[0].msg == 'test-error'

        # NOTE: in this case, we use eventMsg as the action, not the msg
        assert len(base_job.events) == 1
        assert base_job.events[0].action == 'test-event'

    def test_record_result(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.return_value = None

        resultCode = 200
        base_job.record_result(resultCode)

        base_job.save.assert_called_once_with()
        assert base_job.resultCode == resultCode
        assert base_job.completeness == 1

    def test_progress(self, mocker):
        base_job = jobs.BaseJob()

        mocker.patch.object(base_job, 'save')
        base_job.save.return_value = None

        progress_dict = base_job.progress()

        assert progress_dict['completeness'] == 0.0
        assert progress_dict['result'] == -1
        assert len(progress_dict['events']) == 0
        assert len(progress_dict['errors']) == 0

        errorCode = 500
        msg = 'test-error'
        eventMsg = 'test-event'
        base_job.record_error(errorCode, msg, eventMsg)

        resultCode = 200
        base_job.record_result(resultCode)

        progress_dict = base_job.progress()

        assert progress_dict['completeness'] == 1.0
        assert progress_dict['result'] == resultCode
        assert len(progress_dict['events']) == 1
        assert len(progress_dict['errors']) == 1

        assert progress_dict['events'][0]['action'] == 'test-event'
        assert progress_dict['errors'][0]['msg'] == 'test-error'
