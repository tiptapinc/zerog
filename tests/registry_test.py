import pdb
import pytest

from zerog.datastores.mock_datastore import MockDatastore
from zerog.queues.mock_queue import MockQueue
from zerog.registry import JobRegistry, find_subclasses, import_submodules

from tests.job_classes import GoodJob, RequeueJob


def test_initial_registry():
    registry = JobRegistry()
    classes = registry.get_registered_classes()

    assert len(classes) == 0


def test_add_classes(job_registry):
    registry = job_registry(GoodJob)
    classes = registry.get_registered_classes()

    assert len(classes) == 1
    assert GoodJob in classes


def test_add_another_class(job_registry):
    registry = job_registry(GoodJob)
    added = registry.add_classes([RequeueJob])
    classes = registry.get_registered_classes()

    assert len(added) == 1
    assert RequeueJob.__name__ in added
    assert GoodJob.__name__ not in added

    assert len(classes) == 2
    assert GoodJob in classes
    assert RequeueJob in classes


def test_add_not_subclass(job_registry):
    registry = job_registry(GoodJob)
    added = registry.add_classes([int])

    assert len(added) == 1
    assert 'int' in added
    assert added['int']['success'] is False
    assert added['int']['error'] == "NotSubclass"


def test_add_good_and_bad_classes():
    registry = JobRegistry()
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


def test_make_job_with_job_type_arg(job_registry, datastore, jobs_queue):
    registry = job_registry(GoodJob)
    job = registry.make_job(
        dict(), datastore, jobs_queue, jobType=GoodJob.JOB_TYPE
    )
    assert isinstance(job, GoodJob)
    assert job.goodness == "gracious"
    assert job.run() == (200, None)


def test_make_job_with_bad_job_type_arg(job_registry, datastore, jobs_queue):
    registry = job_registry(GoodJob)
    job = registry.make_job(
        dict(), datastore, jobs_queue, jobType="whatever"
    )
    assert job is None


def test_make_job_from_data(job_registry, datastore, jobs_queue):
    registry = job_registry(GoodJob)
    data = dict(goodness="not really", jobType=GoodJob.JOB_TYPE)
    job = registry.make_job(data, datastore, jobs_queue)

    assert isinstance(job, GoodJob)
    assert job.goodness == "not really"
    assert job.run() == (200, None)


def test_get_job(job_registry, datastore, jobs_queue):
    registry = job_registry(GoodJob)
    job = registry.make_job(
        dict(), datastore, jobs_queue, jobType=GoodJob.JOB_TYPE
    )
    job.save()
    job = registry.get_job(job.uuid, datastore, jobs_queue)

    assert isinstance(job, GoodJob)
    assert job.goodness == "gracious"
    assert job.run() == (200, None)


def test_get_job_bad_uuid(job_registry, datastore, jobs_queue):
    registry = job_registry(GoodJob)
    job = registry.make_job(
        dict(), datastore, jobs_queue, jobType=GoodJob.JOB_TYPE
    )
    job.save()
    job = registry.get_job("poopty poopty pants", datastore, jobs_queue)

    assert job is None
