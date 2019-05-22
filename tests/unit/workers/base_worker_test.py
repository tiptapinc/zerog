import beanstalkc
import pytest
import time

# pytest debugger
# use pdb.set_trace() to walk through a test
import pdb

from geyser import jobs
from geyser import geyser_queue
from geyser import workers


#
# Unit tests for base_worker.
#
# Notes:
#   -
#

TEST_QUEUE_NAME = "test_queue"


class WFTestError(Exception):
    pass


class MockJob(jobs.BaseJob):
    def __init__(self, **kwargs):
        super(MockJob, self).__init__(**kwargs)

        self.resumeAt = kwargs.get('resumeAt', time.time())
        self.requeue = kwargs.get('requeue', False)

    def run(self):
        return self.resumeAt, self.requeue


class MockJobError(jobs.BaseJob):
    def __init__(self, **kwargs):
        super(MockJobError, self).__init__(**kwargs)

        self.error = kwargs.get(
            'error',
            geyser_queue.queue_globals.WFErrorFinish,
        )

    def run(self):
        raise self.error


class TestBaseWorker(object):
    def test_initializes(self):
        base_worker = workers.BaseWorker(TEST_QUEUE_NAME)

        assert base_worker.queueName == TEST_QUEUE_NAME

    def test_process_queue_job_success(self, mocker):
        mock_job = MockJob()
        queue_job = beanstalkc.Job(None, 1, f'"{mock_job.uuid}"')

        base_worker = self._set_up_process_queue_mocks(mocker, mock_job)
        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(mock_job, 'enqueue')

        base_worker._process_queue_job(queue_job)

        base_worker.queue.delete.assert_called_once_with(queue_job.jid)
        assert not mock_job.enqueue.called

    def test_process_queue_job_success_requeue(self, mocker):
        mock_job = MockJob(requeue=True)
        queue_job = beanstalkc.Job(None, 1, f'"{mock_job.uuid}"')

        base_worker = self._set_up_process_queue_mocks(mocker, mock_job)
        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(mock_job, 'enqueue')

        base_worker._process_queue_job(queue_job)

        base_worker.queue.delete.assert_called_once_with(queue_job.jid)
        mock_job.enqueue.assert_called_once_with(delay=0)

    def test_process_queue_job_wferror_finish(self, mocker):
        mock_job = MockJobError(error=geyser_queue.queue_globals.WFErrorFinish)
        queue_job = beanstalkc.Job(None, 1, f'"{mock_job.uuid}"')

        base_worker = self._set_up_process_queue_mocks(mocker, mock_job)
        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(mock_job, 'enqueue')

        base_worker._process_queue_job(queue_job)

        base_worker.queue.delete.assert_called_once_with(queue_job.jid)
        assert not mock_job.enqueue.called

    def test_process_queue_job_wferror_continue(self, mocker):
        mock_job = MockJobError(
            error=geyser_queue.queue_globals.WFErrorContinue
        )
        queue_job = beanstalkc.Job(None, 1, f'"{mock_job.uuid}"')

        base_worker = self._set_up_process_queue_mocks(mocker, mock_job)
        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(base_worker.queue, 'release')
        mocker.patch.object(mock_job, 'enqueue')

        base_worker._process_queue_job(queue_job)

        base_worker.queue.release.assert_called_once_with(
            queue_job.jid,
            delay=10,
        )
        assert not base_worker.queue.delete.called
        assert not mock_job.enqueue.called

    def test_process_queue_job_internal_error(self, mocker):
        mock_job = MockJobError(error=WFTestError)
        queue_job = beanstalkc.Job(None, 1, f'"{mock_job.uuid}"')

        base_worker = self._set_up_process_queue_mocks(mocker, mock_job)
        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(base_worker.queue, 'release')
        mocker.patch.object(mock_job, 'enqueue')
        mocker.patch.object(mock_job, 'record_error')

        with pytest.raises(WFTestError):
            base_worker._process_queue_job(queue_job)

            base_worker.queue.release.assert_called_once_with(
                queue_job.jid,
                delay=10,
            )
            mock_job.record_error.assert_called_once()
            assert not base_worker.queue.delete.called
            assert not mock_job.enqueue.called

    def test_manage_retries_job_not_deleted(self, mocker):
        queueJobId = 1
        reserves = 0
        timeouts = 0
        stats = {
            'reserves': reserves,
            'timeouts': timeouts,
        }
        job = MockJob()
        jobId = job.uuid

        base_worker = workers.BaseWorker(TEST_QUEUE_NAME)

        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(job, 'record_result')

        result = base_worker.manage_retries(queueJobId, stats, jobId, job)

        assert result is False
        assert not base_worker.queue.delete.called
        assert not job.record_result.called

    def test_manage_retries_job_too_many_reserves(self, mocker):
        queueJobId = 1
        reserves = 4
        timeouts = 0
        stats = {
            'reserves': reserves,
            'timeouts': timeouts,
        }
        job = MockJob()
        jobId = job.uuid

        base_worker = workers.BaseWorker(TEST_QUEUE_NAME)

        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(job, 'record_result')

        result = base_worker.manage_retries(queueJobId, stats, jobId, job)

        assert result is True

        base_worker.queue.delete.assert_called_once_with(queueJobId)
        job.record_result.assert_called_once_with(
            geyser_queue.queue_globals.INTERNAL_ERROR,
            "more than 3 reserves, deleting from queue",
        )

    def test_manage_retries_job_too_many_timeouts(self, mocker):
        queueJobId = 1
        reserves = 0
        timeouts = 4
        stats = {
            'reserves': reserves,
            'timeouts': timeouts,
        }
        job = MockJob()
        jobId = job.uuid

        base_worker = workers.BaseWorker(TEST_QUEUE_NAME)

        mocker.patch.object(base_worker.queue, 'delete')
        mocker.patch.object(job, 'record_result')

        result = base_worker.manage_retries(queueJobId, stats, jobId, job)

        assert result is True

        base_worker.queue.delete.assert_called_once_with(queueJobId)
        job.record_result.assert_called_once_with(
            geyser_queue.queue_globals.INTERNAL_ERROR,
            "more than 3 timeouts, deleting from queue",
        )

    def _set_up_process_queue_mocks(self, mocker, mock_job):
        mocker.patch(
            'geyser.workers.base_worker.get_base_job',
            return_value=mock_job,
        )

        base_worker = workers.BaseWorker(TEST_QUEUE_NAME)

        mocker.patch.object(base_worker, 'manage_retries', return_value=False)
        mocker.patch.object(
            geyser_queue.sync_queue.QUEUE,
            'stats_job',
            return_value={},
        )

        return base_worker
