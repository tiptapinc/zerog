import pdb
import pytest

import zerog


def test_server_init(make_datastore, make_queue):
    server = zerog.Server(
        "zerog_test",
        make_datastore,
        make_queue,
        [],
        [],
    )
    assert True
