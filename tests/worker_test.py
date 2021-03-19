import json
import pdb
import pytest
import random
import time

from zerog.workers.base import MAX_RESERVES, MAX_TIMEOUTS
from zerog.jobs import INTERNAL_ERROR, NO_RESULT
from zerog.queues.beanstalk_queue import QueueJob

from tests.job_classes import (
    GoodJob,
    RequeueJob,
    ExceptionJob,
    NoReturnValJob,
    ReturnGoodListJob,
    ReturnBadListJob,
    ReturnGoodStringJob,
    ReturnBadStringJob,
    ReturnDictJob,
    JobLogInfoJob,
    WarningFinishJob,
    ErrorContinueJob,
    ErrorFinishJob
)


def test_run_good_job(run_job):
    """
    tests that a simple job runs successfully
    """
    job, queueJob = run_job(GoodJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 200
    assert job.completeness == 1


def test_missing_job(make_job_and_worker, clear_queue):
    """
    tests the case where we the worker can't get the job specified by the
    job uuid in the queue
    """
    job, registry, worker, parentConn = make_job_and_worker(GoodJob)
    clear_queue(job.queue)
    job.enqueue()
    queueJob = job.queue.reserve(timeout=0)

    # make the queueJob defective, then pass to the worker
    queueJob.body = json.dumps(random.randint(1, 1000000))
    worker._process_queue_job(queueJob)
    job.reload()

    # queueJob should have been released back into the queue
    stats = queueJob.stats()
    queueJob.delete()
    assert stats['state'] == 'delayed'
    assert stats['releases'] == 1
    assert stats['reserves'] == 1

    assert job.resultCode == NO_RESULT


@pytest.mark.skip(reason="30 second retry delay makes this test slow")
def test_too_many_reserves(make_job_and_worker, clear_queue):
    """
    Another test of the case where we somehow can't load the zerog job
    that's identified by a queueJob. After some maximum number of reserves,
    the queueJob should be discarded
    """
    job, registry, worker, parentConn = make_job_and_worker(GoodJob)
    clear_queue(job.queue)
    job.enqueue()
    queueJob = job.queue.reserve(timeout=0)

    # make the queueJob defective each time before passing it to worker,
    # then check how many times it gets released before it is deleted
    reserveCount = 1
    for _ in range(MAX_RESERVES + 1):
        queueJob.body = json.dumps(random.randint(1, 1000000))
        worker._process_queue_job(queueJob)
        newQueueJob = job.queue.reserve(timeout=60)
        if newQueueJob:
            assert newQueueJob.body == queueJob.body
            reserveCount += 1
            queueJob = newQueueJob
        else:
            break

    job.reload()
    assert job.queue.reserve(timeout=0) is None
    assert job.resultCode == NO_RESULT
    assert reserveCount == MAX_RESERVES + 1


@pytest.mark.skip(reason="30 second retry delay makes this test slow")
def test_too_many_timeouts(make_job_and_worker, clear_queue):
    """
    Test of the case where we somehow can't load the zerog job, but the
    job was somehow reserved for longer than its queue timeout. After
    some maximum number of timeouts, the queueJob should be discarded.
    """
    job, registry, worker, parentConn = make_job_and_worker(GoodJob)
    clear_queue(job.queue)
    job.enqueue(ttr=1)
    queueJob = job.queue.reserve(timeout=0)
    time.sleep(2)

    # make the queueJob defective each time before passing it to worker,
    # then check how many times it gets released before it is deleted
    reserveCount = 1
    for _ in range(MAX_TIMEOUTS + 1):
        queueJob.body = json.dumps(random.randint(1, 1000000))
        worker._process_queue_job(queueJob)
        newQueueJob = job.queue.reserve(timeout=60)
        if newQueueJob:
            assert newQueueJob.body == queueJob.body
            reserveCount += 1
            queueJob = newQueueJob
        else:
            break

    stats = queueJob.stats()
    assert stats['reserves'] > 0


def test_requeue_job(make_job_and_worker, clear_queue):
    """
    Test to make sure that the worker properly requeues a job that
    runs partially and requests to be requeued
    """
    job, registry, worker, parentConn = make_job_and_worker(RequeueJob)
    clear_queue(job.queue)
    job.enqueue()
    queueJob = job.queue.reserve(timeout=0)

    worker._process_queue_job(queueJob)
    job.reload()

    # RequeueJob requests a delay of 1, so reserve timeout can be short
    newQueueJob = job.queue.reserve(timeout=3)

    assert newQueueJob is not None
    assert newQueueJob.body == queueJob.body
    assert job.completeness == 0.6
    assert job.resultCode == NO_RESULT

    worker._process_queue_job(newQueueJob)
    job.reload()
    newQueueJob = job.queue.reserve(timeout=0)

    assert newQueueJob is None
    assert job.completeness == 1
    assert job.resultCode == 200


def test_exception_job(run_job, peek_delayed):
    """
    tests that an exception job has recorded an exception in its
    errors attribute, that it has been requeued for another try,
    and that its result code shows it's still in process
    """
    job, queueJob = run_job(ExceptionJob)

    # queueJob was deleted, but there should be a newQueueJob ready
    # to be retried
    newQueueJob = peek_delayed(job.queue)
    assert newQueueJob is not None
    stats = newQueueJob.stats()
    newQueueJob.delete()
    assert stats['state'] == 'delayed'
    assert newQueueJob.body == queueJob.body

    assert len(job.errors) == 1
    assert job.errors[0].errorCode == INTERNAL_ERROR
    assert job.resultCode == NO_RESULT
    assert "Traceback" in job.errors[0].msg


def test_no_return_val_job(run_job):
    """
    tests that the worker can handle a job that doesn't return anything
    from its 'run' method
    """
    job, queueJob = run_job(NoReturnValJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 200


def test_return_good_list_job(run_job):
    """
    tests that the worker can handle a job that returns a list with a
    valid resultCode and delay
    """
    job, queueJob = run_job(ReturnGoodListJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 220


def test_return_bad_list_job(run_job):
    """
    tests that the worker can handle a job that returns a list with an
    invalid resultCode and delay
    """
    job, queueJob = run_job(ReturnBadListJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 200


def test_return_good_string_job(run_job):
    """
    tests that the worker can handle a job that returns the resultCode
    as a numeric string
    """
    job, queueJob = run_job(ReturnGoodStringJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 220


def test_return_bad_string_job(run_job):
    """
    tests that the worker can handle a job that returns the resultCode
    as a non-numeric string
    """
    job, queueJob = run_job(ReturnBadStringJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 200


def test_return_dict_job(run_job):
    """
    tests that the worker can handle a job that returns a dictionary
    from its 'run' method
    """
    job, queueJob = run_job(ReturnDictJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 200


def test_job_log_info_job(run_job):
    """
    tests that a job can log an event
    """
    job, queueJob = run_job(JobLogInfoJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert len(job.events) == 1
    assert job.resultCode == 200


def test_warning_finish_job(run_job):
    """
    tests that a job can raise a 'warning finish' exception and
    the worker will catch and handle it
    """
    job, queueJob = run_job(WarningFinishJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 291
    assert len(job.warnings) == 1
    assert job.warnings[0].msg == "warning, mon"


def test_error_continue_job(run_job, peek_delayed):
    """
    tests that a job can raise an 'error continue' exception and
    the worker will catch and handle it
    """
    job, queueJob = run_job(ErrorContinueJob)

    # queueJob was deleted, but there should be a newQueueJob ready
    # to be retried
    newQueueJob = peek_delayed(job.queue)
    assert newQueueJob is not None
    stats = newQueueJob.stats()
    newQueueJob.delete()
    assert stats['state'] == 'delayed'
    assert newQueueJob.body == queueJob.body

    assert len(job.errors) == 1
    assert job.errors[0].errorCode == 512
    assert job.errors[0].msg == "it errored, dude"
    assert job.resultCode == NO_RESULT


def test_error_finish_job(run_job):
    """
    tests that a job can raise an 'error finish' exception and
    the worker will catch and handle it
    """
    job, queueJob = run_job(ErrorFinishJob)

    newQueueJob = job.queue.reserve(timeout=0)
    assert newQueueJob is None
    assert job.resultCode == 476
    assert len(job.errors) == 1
    assert job.errors[0].errorCode == 476
    assert job.errors[0].msg == "it errored to death, chum"
