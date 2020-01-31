import mock
import multiprocessing
import pdb
import pytest

from ..registry import JobRegistry
from ..server import Server
from ..workers import BaseWorker

from .job_classes import GoodJob
from .mock_datastore import MockDatastore
from .mock_queue import MockQueue, MockQueueJob


@pytest.fixture
def good_job_registry():
    """
    Creates a JobRegistry and adds the GoodJob class to it
    """
    registry = JobRegistry(MockDatastore(), MockQueue())
    registry.add_classes([GoodJob])
    return registry


@pytest.fixture
def good_job_registry_with_keepalive():
    keepalive = mock.Mock()
    registry = JobRegistry(MockDatastore(), MockQueue(), keepalive)
    registry.add_classes([GoodJob])
    return registry, keepalive


@pytest.fixture
def make_good_job(good_job_registry):
    registry = good_job_registry
    job = registry.make_job(dict(), GoodJob.JOB_TYPE)
    return job, registry


@pytest.fixture
def make_good_job_with_keepalive(good_job_registry_with_keepalive):
    registry, keepalive = good_job_registry_with_keepalive
    job = registry.make_job(dict(), GoodJob.JOB_TYPE)
    return job, registry, keepalive


@pytest.fixture()
def make_test_job_registry():
    """
    Creates a JobRegistry and adds a test-defined job to it
    """
    def _func(jobClass):
        registry = JobRegistry(MockDatastore(), MockQueue())
        registry.add_classes([jobClass])
        return registry

    return _func


@pytest.fixture()
def make_test_job(make_test_job_registry):
    def _func(jobClass):
        registry = make_test_job_registry(jobClass)
        job = registry.make_job(dict(), jobClass.JOB_TYPE)
        return job, registry

    return _func


@pytest.fixture
def make_worker():
    def _func(registry):
        parentConn, childConn = multiprocessing.Pipe()
        worker = BaseWorker(
            registry.datastore,
            registry.queue,
            registry,
            childConn
        )
        return worker

    return _func


@pytest.fixture
def make_job_and_worker(make_test_job, make_worker):
    def _func(jobClass):
        job, registry = make_test_job(jobClass)
        job.save()
        worker = make_worker(registry)
        return job, registry, worker

    return _func


@pytest.fixture
def run_job(make_job_and_worker):
    def _func(jobClass):
        job, registry, worker = make_job_and_worker(jobClass)
        queueJob = MockQueueJob(registry.queue, 1, job.uuid)
        worker._process_queue_job(queueJob)
        job.reload()
        releaseJob = registry.queue.reserve()
        return job, queueJob, releaseJob

    return _func


@pytest.fixture
def zerog_app():
    """
    Creates a zerog app with specified handlers
    """
    def _func(jobClasses, handlers):
        # pdb.set_trace()
        server = Server(
            MockDatastore(),
            MockQueue(),
            MockQueue(),
            jobClasses,
            handlers
        )
        return server

    return _func
