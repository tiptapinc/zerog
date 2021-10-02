import pdb
import pytest

from zerog.datastores.couchbase_datastore import CouchbaseDatastore


RANDOM_PHRASES = {
    "a": "gauges self-punishment",
    "b": "disruptors Piccadilly",
    "c": "encyclopedic impassiveness",
    "d": "liquidate attenuated",
    "e": "steps prospects",
    "f": "late-term servomotor",
    "g": "off-drive Special Forces",
    "h": "databank dead-wood",
    "i": "bondswoman distant",
    "j": "drop-dead Pohang"
}


def test_init_datastore(datastore):
    assert isinstance(datastore, CouchbaseDatastore)


def test_delete(datastore):
    key = "test_delete"
    value = {"this": "doesn't matter"}

    success = datastore.set(key, value)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == value

    success = datastore.delete(key)
    assert success is True

    value = datastore.read(key)
    assert not value


def test_delete_nonexistent(datastore):
    key = "test_delete"
    value = {"this": "still doesn't matter"}

    success = datastore.set(key, value)
    assert success is True
    datastore.delete(key)

    success = datastore.delete(key)
    assert success is False


def test_create_and_read_str(datastore):
    key = "test_string"
    value = "the quick brown fox jumps over the lazy dog"

    datastore.delete(key)  # ensure it's not already there
    success = datastore.create(key, value)

    assert success is True

    readvalue = datastore.read(key)
    assert isinstance(readvalue, str)
    assert readvalue == value


def test_create_and_read_dict(datastore):
    key = "test_dict"
    value = {"yo": "dawg", "fish": "marlin", "repo man": "intense"}

    datastore.delete(key)  # ensure it's not already there
    success = datastore.create(key, value)

    assert success is True

    readvalue = datastore.read(key)
    assert isinstance(readvalue, dict)
    assert readvalue == value


def test_create_preexisting(datastore):
    key = "test_string"
    value = "the quick brown fox has a belly ache"

    with pytest.raises(Exception):
        datastore.create(key, value)


def test_read_nonexistent(datastore):
    key = "test_string"
    datastore.delete(key)  # ensure it's not already there

    success = datastore.read(key)
    assert not success


def test_read_with_cas(datastore):
    key = "test_string"
    value = "the quick brown fox jumps on the lazy dog"

    success = datastore.set(key, value)
    assert success is True

    readvalue, readcas = datastore.read_with_cas(key)
    assert readvalue == value
    assert bool(readcas)    # ensure it's not nothing


def test_update(datastore):
    key = "test_string"
    value = "the quick brown fox laughs at the lazy dog"

    success = datastore.set(key, value)
    assert success is True

    newvalue = value.replace("fox", "aardvark")
    success = datastore.update(key, newvalue)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_update_with_cas(datastore):
    key = "test_string"
    value = "the quick brown fox wiggles under the lazy dog"

    datastore.delete(key)  # ensure it's not already there
    success, cas = datastore.set_with_cas(key, value)
    assert success is True

    newvalue = "the slow red fox jumps on top of the crazy dog"
    with pytest.raises(datastore.casException):
        datastore.update_with_cas(key, newvalue, cas=55)

    readvalue = datastore.read(key)
    assert readvalue == value

    success, newcas = datastore.update_with_cas(key, newvalue, cas=cas)
    assert success is True
    assert newcas != cas

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_set(datastore):
    key = "test_string"
    value = "the quirky brown fox pole vaults the Lays-eating dog"

    datastore.delete(key)
    success = datastore.set(key, value)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == value


def test_set_preexisting(datastore):
    key = "test_string"
    value = "the spritely fox bounds over the somnolent dog"

    datastore.delete(key)
    success = datastore.set(key, value)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == value

    newvalue = "the alaskan fox shakes hands with the hawaiian dog"
    success = datastore.set(key, newvalue)
    assert success is True

    readvalue = datastore.read(key)
    assert readvalue == newvalue


def test_set_with_cas(datastore):
    key = "test_string"
    value = "the floundering fox jumps over the two-dimensional dog"

    datastore.delete(key)  # ensure it's not already there
    success, cas = datastore.set_with_cas(key, value)
    assert success is True

    newvalue = "the floundering fox jumps over the flounder"
    with pytest.raises(datastore.casException):
        datastore.set_with_cas(key, newvalue, cas=55)

    readvalue = datastore.read(key)
    assert readvalue == value

    success, newcas = datastore.set_with_cas(key, newvalue, cas=cas)
    assert success is True
    assert newcas != cas

    readvalue = datastore.read(key)
    assert readvalue == newvalue
