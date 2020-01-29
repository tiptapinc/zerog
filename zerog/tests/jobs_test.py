import datetime
import pdb
import pytest

from ..jobs import INTERNAL_ERROR, NO_RESULT
from ..jobs.base import ErrorContinue, ErrorFinish, WarningFinish

from .job_classes import GoodJob, NoRunJob, NoJobTypeJob, NoSchemaJob
from .mock_datastore import MockDatastore
from .mock_queue import MockQueue


def test_good_job_is_good(make_good_job):
    job, registry = make_good_job

    assert isinstance(job, GoodJob)
    assert job.goodness == "gracious"
    assert job.run() == 200


def test_no_run_job_is_bad():
    with pytest.raises(TypeError):
        NoRunJob(MockDatastore(), MockQueue())


def test_no_job_type_job_is_bad():
    with pytest.raises(TypeError):
        NoJobTypeJob(MockDatastore(), MockQueue())


def test_no_schema_job_is_bad():
    with pytest.raises(TypeError):
        NoSchemaJob(MockDatastore(), MockQueue())


def test_no_save(make_good_job):
    job, registry = make_good_job
    job2 = registry.get_job(job.uuid)

    assert job2 is None


def test_save(make_good_job):
    job, registry = make_good_job
    job.save()
    job2 = registry.get_job(job.uuid)

    assert job2 is not None
    assert job.uuid == job2.uuid


def test_save_change_save(make_good_job):
    job, registry = make_good_job
    job.resultCode = 200
    job.save()
    job = registry.get_job(job.uuid)

    assert job.resultCode == 200


def test_update_attrs(make_good_job):
    job, registry = make_good_job
    job.update_attrs(resultCode=200)
    job2 = registry.get_job(job.uuid)

    assert job2 is not None
    assert job2.resultCode == 200


def test_save_collision_fails(make_good_job):
    job, registry = make_good_job
    job.save()
    job2 = registry.get_job(job.uuid)
    job.update_attrs(resultCode=200)

    with pytest.raises(MockDatastore.casException):
        job2.save()


def test_update_attrs_collision_succeeds(make_good_job):
    job, registry = make_good_job
    job.save()
    job2 = registry.get_job(job.uuid)
    job.update_attrs(resultCode=200)
    job2.update_attrs(resultCode=218)
    job = registry.get_job(job.uuid)

    assert job.resultCode == 218


def test_reload(make_good_job):
    job, registry = make_good_job
    job.save()
    job2 = registry.get_job(job.uuid)
    job.update_attrs(resultCode=200)

    assert job2.resultCode == -1

    job2.reload()

    assert job2.resultCode == 200


def test_keep_alive(make_good_job_with_keepalive):
    job, registry, keepalive = make_good_job_with_keepalive
    job.keep_alive()

    keepalive.assert_called_with()


def test_set_completeness(make_good_job_with_keepalive):
    completeness = 0.2
    job, registry, keepalive = make_good_job_with_keepalive
    job.set_completeness(completeness)

    assert job.completeness == completeness
    keepalive.assert_called_with()


def test_set_completeness_too_low(make_good_job_with_keepalive):
    completeness = -1.2
    job, registry, keepalive = make_good_job_with_keepalive
    job.set_completeness(completeness)

    assert job.completeness == 0
    keepalive.assert_called_with()


def test_set_completeness_too_high(make_good_job_with_keepalive):
    completeness = 18
    job, registry, keepalive = make_good_job_with_keepalive
    job.set_completeness(completeness)

    assert job.completeness == 1
    keepalive.assert_called_with()


def test_add_to_completeness(make_good_job_with_keepalive):
    delta = 0.4
    job, registry, keepalive = make_good_job_with_keepalive
    job.add_to_completeness(delta)

    assert job.completeness == delta
    keepalive.assert_called_with()


def test_add_too_much_to_completeness(make_good_job_with_keepalive):
    completeness = 0.5
    delta = 0.9
    job, registry, keepalive = make_good_job_with_keepalive
    job.set_completeness(completeness)

    assert job.completeness == completeness

    job.add_to_completeness(delta)

    assert job.completeness == 1
    keepalive.assert_called_with()


def test_subtract_too_much_from_completeness(make_good_job_with_keepalive):
    completeness = 0.5
    delta = -0.9
    job, registry, keepalive = make_good_job_with_keepalive
    job.set_completeness(completeness)

    assert job.completeness == completeness

    job.add_to_completeness(delta)

    assert job.completeness == 0
    keepalive.assert_called_with()


def test_tick(make_good_job_with_keepalive):
    tickval = 0.006
    job, registry, keepalive = make_good_job_with_keepalive
    job.set_tick_value(tickval)

    assert job.tickval == tickval

    job.tick()

    assert job.completeness == 0

    job.tick()

    assert job.completeness == tickval * 2

    job.tick()

    assert job.completeness == tickval * 2

    job.tick()

    assert job.completeness == tickval * 4


def test_job_log_info(make_good_job):
    msg = "test info message"
    job, registry = make_good_job
    job.save()
    now = datetime.datetime.utcnow()
    job.job_log_info(msg)

    assert len(job.events) == 1
    assert job.events[0].timeStamp - now < datetime.timedelta(seconds=1)
    assert job.events[0].msg == msg


def test_job_log_warning(make_good_job):
    msg = "test warning message"
    job, registry = make_good_job
    job.save()
    now = datetime.datetime.utcnow()
    job.job_log_warning(msg)

    assert len(job.warnings) == 1
    assert job.warnings[0].timeStamp - now < datetime.timedelta(seconds=1)
    assert job.warnings[0].msg == msg


def test_job_log_error(make_good_job):
    msg = "test error message"
    job, registry = make_good_job
    job.save()
    now = datetime.datetime.utcnow()
    job.job_log_error(INTERNAL_ERROR, msg)

    assert len(job.errors) == 1
    assert job.errors[0].timeStamp - now < datetime.timedelta(seconds=1)
    assert job.errors[0].errorCode == INTERNAL_ERROR
    assert job.resultCode == NO_RESULT
    assert job.errors[0].msg == msg


def test_raise_warning_finish(make_good_job):
    msg = "test warning message"
    resultCode = 200
    job, registry = make_good_job
    job.save()
    now = datetime.datetime.utcnow()

    with pytest.raises(WarningFinish):
        job.raise_warning_finish(resultCode, msg)

    assert job.resultCode == resultCode
    assert job.completeness == 1
    assert len(job.warnings) == 1
    assert job.warnings[0].timeStamp - now < datetime.timedelta(seconds=1)
    assert job.warnings[0].msg == msg


def test_raise_error_continue(make_good_job):
    msg = "test error message"
    errorCode = INTERNAL_ERROR
    job, registry = make_good_job
    job.save()
    now = datetime.datetime.utcnow()

    with pytest.raises(ErrorContinue):
        job.raise_error_continue(errorCode, msg)

    assert len(job.errors) == 1
    assert job.errors[0].timeStamp - now < datetime.timedelta(seconds=1)
    assert job.errors[0].errorCode == errorCode
    assert job.errors[0].msg == msg


def test_raise_error_finish(make_good_job):
    msg = "test error message"
    errorCode = INTERNAL_ERROR
    job, registry = make_good_job
    job.save()
    now = datetime.datetime.utcnow()

    with pytest.raises(ErrorFinish):
        job.raise_error_finish(errorCode, msg)

    assert job.resultCode == errorCode
    assert job.completeness == 1
    assert len(job.errors) == 1
    assert job.errors[0].timeStamp - now < datetime.timedelta(seconds=1)
    assert job.errors[0].errorCode == errorCode
    assert job.errors[0].msg == msg
