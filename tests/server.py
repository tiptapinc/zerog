import pdb
import pytest

import tests.job_classes
import zerog


def make_datastore():
    return zerog.CouchbaseDatastore(
        "couchbase", "Administrator", "password", "test"
    )


def make_queue(queueName):
    return zerog.BeanstalkdQueue("beanstalkd", 11300, queueName)


def make_app():
    jobClasses = zerog.find_subclasses(zerog.BaseJob)
    return zerog.Server(
        "zerog_test", make_datastore, make_queue, jobClasses, []
    )
