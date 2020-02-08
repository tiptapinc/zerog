import pdb
import pytest

from zerog.server import Server

from tests.mock_datastore import MockDatastore
from tests.mock_queue import MockQueue


def test_server_init():
    server = Server(
        MockDatastore(),
        MockQueue(),
        MockQueue(),
        []
    )
    assert True
