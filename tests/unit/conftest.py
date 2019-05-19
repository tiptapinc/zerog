# Unit test shared fixtures

import pytest

from geyser import datastore_configs


@pytest.fixture(scope="session")
def setup_database_globals():
    datastore_configs.set_datastore_globals()
