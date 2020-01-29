import pdb
import pytest

from ..registry import JobRegistry, find_subclasses, import_submodules

from .job_classes import GoodJob, NoRunJob, NoJobTypeJob, NoSchemaJob
from .mock_datastore import MockDatastore
from .mock_queue import MockQueue


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
    added = registry.add_classes([NoRunJob])
    classes = registry.get_registered_classes()

    assert len(added) == 1
    assert NoRunJob.__name__ in added
    assert GoodJob.__name__ not in added

    assert len(classes) == 2
    assert GoodJob in classes
    assert NoRunJob in classes


def test_add_not_subclass(good_job_registry):
    registry = good_job_registry
    added = registry.add_classes([int])

    assert len(added) == 1
    assert 'int' in added
    assert added['int']['success'] is False
    assert added['int']['error'] == "NotSubclass"


def test_add_no_job_type_class(good_job_registry):
    registry = good_job_registry
    added = registry.add_classes([NoJobTypeJob])
    name = NoJobTypeJob.__name__

    assert len(added) == 1
    assert name in added
    assert added[name]['success'] is False
    assert added[name]['error'] == "NoJobType"


def test_add_no_schema_class(good_job_registry):
    registry = good_job_registry
    added = registry.add_classes([NoSchemaJob])
    name = NoSchemaJob.__name__

    assert len(added) == 1
    assert name in added
    assert added[name]['success'] is False
    assert added[name]['error'] == "NoSchema"


def test_add_good_and_bad_classes():
    registry = JobRegistry(MockDatastore(), MockQueue())
    added = registry.add_classes(
        [GoodJob, int, NoJobTypeJob, NoSchemaJob]
    )

    assert len(added) == 4

    name = GoodJob.__name__
    assert name in added
    assert added[name]['success'] is True
    assert added[name]['error'] is None

    expected = [
        (int, "NotSubclass"),
        (NoJobTypeJob, "NoJobType"),
        (NoSchemaJob, "NoSchema")
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
