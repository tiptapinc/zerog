# Unit test shared fixtures

import pytest

from geyser import datastore_configs

from geyser.geyser_queue import queue_handler


TEST_QUEUE_NAME = "test_queue"


@pytest.fixture(autouse=True)
def run_before_all_tests():
    clear_tube()


@pytest.fixture(scope="session")
def setup_database_globals():
    datastore_configs.set_datastore_globals()


def clear_tube():
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
