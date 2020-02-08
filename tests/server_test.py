import pdb
import pytest

from zerog.datastores.mock_datastore import MockDatastore
from zerog.queues.mock_queue import MockQueue
from zerog.server import Server


def test_server_init():
    server = Server(
        MockDatastore(),
        MockQueue(),
        MockQueue(),
        []
    )
    assert True
