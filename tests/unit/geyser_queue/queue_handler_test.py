import beanstalkc
import pytest

# pytest debugger
# use pdb.set_trace() to walk through a test
import pdb

from geyser.geyser_queue import queue_handler

import logging
log = logging.getLogger(queue_handler.__name__)

#
# Unit tests for queue_handler.
#

TEST_QUEUE_NAME = "test_queue"


class TestQueueHandler(object):
    def test_initializes(self):
        queue = queue_handler.QueueHandler(TEST_QUEUE_NAME)

        assert type(queue.queue) == beanstalkc.Connection
        assert queue.queue.using() == TEST_QUEUE_NAME
        assert TEST_QUEUE_NAME in queue.queue.watching()

        self._clear_tube()

    def test_process_queue_job_raises_not_implemented(self):
        queue = queue_handler.QueueHandler(TEST_QUEUE_NAME)
        queue_job = beanstalkc.Job(None, 1, f'"job_uuid"')

        with pytest.raises(NotImplementedError):
            queue._process_queue_job(queue_job)

        self._clear_tube()

    def test_consume_with_no_job(self, mocker):
        queue = queue_handler.QueueHandler(TEST_QUEUE_NAME)

        mocker.patch.object(queue, '_reconsume')
        mocker.patch.object(queue, '_process_queue_job')

        queue._consume()

        queue._reconsume.assert_called_once()
        assert not queue._process_queue_job.called

        self._clear_tube()

    def test_consume_with_job(self, mocker):
        queue = queue_handler.QueueHandler(TEST_QUEUE_NAME)
        jsonJob = f'"job_uuid"'

        mocker.patch.object(queue, '_reconsume')
        mocker.patch.object(queue, '_process_queue_job')

        queue.queue.put(jsonJob)
        queue._consume()

        queue._process_queue_job.assert_called_once()
        assert not queue._reconsume.called

        self._clear_tube()

    def test_put_no_error(self, mocker):
        queue = queue_handler.QueueHandler(TEST_QUEUE_NAME)
        jsonJob = f'"job_uuid"'

        mocker.patch.object(queue, '_reconsume')
        mocker.patch.object(queue, '_process_queue_job')

        queue.put(jsonJob)
        queue._consume()

        queue._process_queue_job.assert_called_once()
        assert not queue._reconsume.called

        self._clear_tube()

    def test_put_with_error(self, mocker):
        queue = queue_handler.QueueHandler(TEST_QUEUE_NAME)
        jsonJob = f'"job_uuid"'

        mocker.patch.object(queue, '_process_queue_job')
        mocker.patch.object(queue.queue, 'put')
        mocker.patch.object(log, 'warning')

        queue.queue.put.return_value = Exception()

        queue.put(jsonJob)
        queue._consume()

        log.warning.assert_called_once()

        self._clear_tube()

    def _clear_tube(self):
        """
        This is a necessary teardown function because tests mock
        _process_queue_job, and so jobs are never otherwise deleted from
        beanstalk tubes.
        """
        queue = queue_handler.QueueHandler(TEST_QUEUE_NAME)

        # poll and delete jobs until there are no more left
        job = queue.queue.reserve(timeout=0)
        while job is not None:
            job.delete()
            job = queue.queue.reserve(timeout=0)
