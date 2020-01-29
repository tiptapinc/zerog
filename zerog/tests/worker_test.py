import pdb
import pytest

from ..workers.base import MAX_RESERVES, MAX_TIMEOUTS
from ..jobs import INTERNAL_ERROR, NO_RESULT

from .job_classes import (
    GoodJob,
    RequeueJob,
    ExceptionJob,
    NoReturnValJob,
    JobLogInfoJob,
    WarningFinishJob,
    ErrorContinueJob,
    ErrorFinishJob
)
from .mock_queue import MockQueueJob


def test_run_good_job(make_job_and_worker):
    job, registry, worker = make_job_and_worker(GoodJob)
    job.save()
    queueJob = MockQueueJob(registry.queue, 1, job.uuid)
    worker._process_queue_job(queueJob)
    job.reload()

    assert registry.queue.reserve() is None
    assert job.resultCode is 200
    assert job.completeness == 1


def test_missing_job(make_job_and_worker):
    job, registry, worker = make_job_and_worker(GoodJob)
    queueJob = MockQueueJob(registry.queue, 1, "not really a uuid")
    worker._process_queue_job(queueJob)
    job.reload()

    # queueJob should have been released back into the queue
    releaseJob = registry.queue.reserve()

    assert releaseJob is not None
    assert releaseJob.body == queueJob.body


def test_too_many_reserves(make_job_and_worker):
    msg = "more than %s reserves" % MAX_RESERVES    # fragile test
    job, registry, worker = make_job_and_worker(GoodJob)
    queueJob = MockQueueJob(registry.queue, 1, job.uuid)
    queueJob.reserves = MAX_RESERVES + 1
    worker._process_queue_job(queueJob)
    job.reload()

    assert registry.queue.reserve() is None
    assert job.resultCode == INTERNAL_ERROR
    assert len(job.errors) > 0
    assert job.errors[0].errorCode == INTERNAL_ERROR
    assert msg in job.errors[0].msg


def test_too_many_timeouts(make_job_and_worker):
    msg = "more than %s timeouts" % MAX_TIMEOUTS    # fragile test
    job, registry, worker = make_job_and_worker(GoodJob)
    queueJob = MockQueueJob(registry.queue, 1, job.uuid)
    queueJob.timeouts = MAX_TIMEOUTS + 1
    worker._process_queue_job(queueJob)
    job.reload()
    releaseJob = registry.queue.reserve()

    assert releaseJob is None
    assert job.resultCode == INTERNAL_ERROR
    assert len(job.errors) == 1
    assert job.errors[0].errorCode == INTERNAL_ERROR
    assert msg in job.errors[0].msg


def test_requeue_job(make_job_and_worker):
    job, registry, worker = make_job_and_worker(RequeueJob)
    queueJob = MockQueueJob(registry.queue, 1, job.uuid)
    worker._process_queue_job(queueJob)
    job.reload()
    releaseJob = registry.queue.reserve()

    assert releaseJob is not None
    assert releaseJob.body == queueJob.body
    assert job.completeness == 0.6
    assert job.resultCode == NO_RESULT

    worker._process_queue_job(releaseJob)
    job.reload()
    releaseJob = registry.queue.reserve()

    assert releaseJob is None
    assert job.completeness == 1
    assert job.resultCode == 200


def test_exception_job(run_job):
    job, queueJob, releaseJob = run_job(ExceptionJob)

    assert releaseJob is not None
    assert releaseJob.body == queueJob.body
    assert len(job.errors) == 1
    assert job.errors[0].errorCode == INTERNAL_ERROR
    assert job.resultCode == NO_RESULT
    assert "Traceback" in job.errors[0].msg


def test_no_return_val_job(run_job):
    job, queueJob, releaseJob = run_job(NoReturnValJob)

    assert releaseJob is None
    assert job.resultCode == 200


def test_job_log_info_job(run_job):
    job, queueJob, releaseJob = run_job(JobLogInfoJob)

    assert releaseJob is None
    assert len(job.events) == 1
    assert job.resultCode == 200


def test_warning_finish_job(run_job):
    job, queueJob, releaseJob = run_job(WarningFinishJob)

    assert releaseJob is None
    assert job.resultCode == 291
    assert len(job.warnings) == 1
    assert job.warnings[0].msg == "warning, mon"


def test_error_continue_job(run_job):
    job, queueJob, releaseJob = run_job(ErrorContinueJob)

    assert releaseJob is not None
    assert releaseJob.body == queueJob.body
    assert len(job.errors) == 1
    assert job.errors[0].errorCode == 512
    assert job.errors[0].msg == "it errored, dude"
    assert job.resultCode == NO_RESULT


def test_error_finish_job(run_job):
    job, queueJob, releaseJob = run_job(ErrorFinishJob)

    assert releaseJob is None
    assert job.resultCode == 476
    assert len(job.errors) == 1
    assert job.errors[0].errorCode == 476
    assert job.errors[0].msg == "it errored to death, chum"
