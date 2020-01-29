import pdb
import pytest

from ..server import Server

from .mock_datastore import MockDatastore
from .mock_queue import MockQueue


def test_server_init():
    server = Server(
        MockDatastore(),
        MockQueue(),
        MockQueue(),
        []
    )
    assert True
