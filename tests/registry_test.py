import pdb
import pytest

from zerog.registry import JobRegistry, find_subclasses, import_submodules

from tests.job_classes import GoodJob, RequeueJob
from tests.mock_datastore import MockDatastore
from tests.mock_queue import MockQueue


def test_initial_registry():
    registry = JobRegistry(MockDatastore(), MockQueue())
    classes = registry.get_registered_classes()

    assert len(classes) == 0


def test_add_classes(good_job_registry):
    registry = good_job_registry
    classes = registry.get_registered_classes()

    assert len(classes) == 1
    assert GoodJob in classes


def test_add_another_class(good_job_registry):
    registry = good_job_registry
    added = registry.add_classes([RequeueJob])
    classes = registry.get_registered_classes()

    assert len(added) == 1
    assert RequeueJob.__name__ in added
    assert GoodJob.__name__ not in added

    assert len(classes) == 2
    assert GoodJob in classes
    assert RequeueJob in classes


def test_add_not_subclass(good_job_registry):
    registry = good_job_registry
    added = registry.add_classes([int])

    assert len(added) == 1
    assert 'int' in added
    assert added['int']['success'] is False
    assert added['int']['error'] == "NotSubclass"


def test_add_good_and_bad_classes():
    registry = JobRegistry(MockDatastore(), MockQueue())
    added = registry.add_classes(
        [GoodJob, int]
    )

    assert len(added) == 2

    name = GoodJob.__name__
    assert name in added
    assert added[name]['success'] is True
    assert added[name]['error'] is None

    expected = [
        (int, "NotSubclass")
    ]
    for jobClass, error in expected:
        name = jobClass.__name__
        assert name in added
        assert added[name]['success'] is False
        assert added[name]['error'] == error


def test_make_job_with_job_type_arg(good_job_registry):
    registry = good_job_registry
    job = registry.make_job(dict(), GoodJob.JOB_TYPE)

    assert isinstance(job, GoodJob)
    assert job.goodness == "gracious"
    assert job.run() == 200


def test_make_job_with_bad_job_type_arg(good_job_registry):
    registry = good_job_registry
    job = registry.make_job(dict(), "whatever")

    assert job is None


def test_make_job_from_data(good_job_registry):
    registry = good_job_registry
    data = dict(goodness="not really", jobType=GoodJob.JOB_TYPE)
    job = registry.make_job(data)

    assert isinstance(job, GoodJob)
    assert job.goodness == "not really"
    assert job.run() == 200


def test_get_job(good_job_registry):
    registry = good_job_registry
    job = registry.make_job(dict(), GoodJob.JOB_TYPE)
    job.save()
    job = registry.get_job(job.uuid)

    assert isinstance(job, GoodJob)
    assert job.goodness == "gracious"
    assert job.run() == 200


def test_get_job_bad_uuid(good_job_registry):
    registry = good_job_registry
    job = registry.make_job(dict(), GoodJob.JOB_TYPE)
    # pdb.set_trace()
    job.save()
    job = registry.get_job("poopty poopty pants")

    assert job is None
