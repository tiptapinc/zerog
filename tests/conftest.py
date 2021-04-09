import mock
import multiprocessing
import pdb
import pytest

from tests.job_classes import GoodJob
import zerog
from zerog.queues.beanstalk_queue import QueueJob


@pytest.fixture
def datastore():
    return zerog.CouchbaseDatastore(
        "couchbase", "Administrator", "password", "test"
    )


@pytest.fixture
def make_datastore(datastore):
    def _func():
        return datastore

    return _func


@pytest.fixture
def make_queue():
    """
    creates a beanstalkd queue object

    assumes a docker-compose environment with beanstalkd container at
    hostname "beanstalkd"
    """
    def _func(queueName):
        return zerog.BeanstalkdQueue("beanstalkd", 11300, queueName)

    return _func


@pytest.fixture
def peek_delayed():
    def _func(queue):
        return queue.do_bean("peek_delayed")

    return _func


@pytest.fixture
def jobs_queue(make_queue):
    return make_queue("zerog_test_jobs")


@pytest.fixture
def clear_queue():
    """
    clear all the jobs out of a queue

    call this before enqueuing a job to make sure it's the next job
    to get run.
    """
    def _func(queue):
        queue.attach()
        while True:
            j = queue.reserve(timeout=0)
            if j is None:
                break

            j.delete()

    return _func


@pytest.fixture
def job_registry():
    def _func(jobClass):
        registry = zerog.JobRegistry()
        registry.add_classes([jobClass])
        return registry

    return _func


@pytest.fixture()
def make_test_job(job_registry, datastore, jobs_queue):
    def _func(jobClass, **kwargs):
        registry = job_registry(jobClass)
        job = registry.make_job(
            dict(), datastore, jobs_queue, jobType=jobClass.JOB_TYPE, **kwargs
        )
        return job, registry

    return _func


@pytest.fixture
def make_good_job(make_test_job):
    return make_test_job(GoodJob)


@pytest.fixture
def make_good_job_with_keepalive(make_test_job):
    keepalive = mock.Mock()
    return make_test_job(GoodJob, keepalive=keepalive) + (keepalive,)


@pytest.fixture
def make_worker(make_datastore, make_queue):
    """
    makes a worker and initializes its 'run' context
    """
    def _func(registry):
        parentConn, childConn = multiprocessing.Pipe()
        worker = zerog.BaseWorker(
            "zerog_test",
            make_datastore,
            make_queue,
            registry,
            childConn
        )
        worker.run_init()
        return worker, parentConn

    return _func


@pytest.fixture
def make_job_and_worker(make_test_job, make_worker):
    def _func(jobClass):
        job, registry = make_test_job(jobClass)
        job.save()
        worker, parentConn = make_worker(registry)
        return job, registry, worker, parentConn

    return _func


@pytest.fixture
def run_job(make_job_and_worker, clear_queue):
    """
    creates a job
    creates a worker
    clears the queue
    enqueues the job
    simulates running the worker's polling loop
    """
    def _func(jobClass):
        job, registry, worker, parentConn = make_job_and_worker(jobClass)
        clear_queue(job.queue)
        job.enqueue()
        queueJob = job.queue.reserve(timeout=0)
        worker._process_queue_job(queueJob)
        job.reload()
        return job, queueJob

    return _func


@pytest.fixture
def zerog_app(make_datastore, make_queue, clear_queue):
    """
    Creates a zerog app with specified handlers
    """
    server = None
    def _func(jobClasses, handlers):
        nonlocal server
        server = zerog.Server(
            "zerog_test",
            make_datastore,
            make_queue,
            jobClasses,
            handlers,
            thisHost="zerog"
        )
        return server

    yield _func
    server.kill_worker()
    clear_queue(server.ctrlChannel.queue)
