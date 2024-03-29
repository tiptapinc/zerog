#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import json
import pdb
import pytest
from tornado.httpclient import HTTPError

from zerog.handlers.uuid import (
    ProgressHandler, InfoHandler, GetDataHandler, UUID_PATT
)
from zerog.handlers.run_job import RunJobHandler
from zerog.jobs import BaseJob
from zerog.registry import find_subclasses

from tests import job_classes


# These tests vary from the "kwarg_handlers" tests only in the endpoint
# patterns, which test different paths to extract the uuid in the handler
#
# They need their own separate file because of limitations in the 'app'
# fixture

@pytest.fixture
def app(zerog_app):
    jobClasses = find_subclasses(BaseJob)
    handlers = [
        ("/progress/([^/]+)", ProgressHandler),
        ("/info/([^/]+)", InfoHandler),
        ("/data/([^/]+)", GetDataHandler),
        ("/runjob/([^/]+)", RunJobHandler)
    ]
    app = zerog_app(jobClasses, handlers)
    return app


@pytest.mark.gen_test
def test_progress(app, http_client, base_url, make_test_job):
    job, registry = make_test_job(job_classes.GoodJob)
    job.save()
    response = yield http_client.fetch("%s/progress/%s" % (base_url, job.uuid))

    assert response.code == 200

    progress = json.loads(response.body)
    for key in ["completeness", "result"]:
        assert key in progress


@pytest.mark.gen_test
def test_info(app, http_client, base_url, make_test_job):
    job, registry = make_test_job(job_classes.GoodJob)
    job.save()
    response = yield http_client.fetch("%s/info/%s" % (base_url, job.uuid))

    assert response.code == 200

    info = json.loads(response.body)
    for key in ["completeness", "result", "events", "errors", "warnings"]:
        assert key in info


@pytest.mark.gen_test
def test_data(app, http_client, base_url, make_test_job):
    job, registry = make_test_job(job_classes.GoodJob)
    job.save()
    response = yield http_client.fetch("%s/data/%s" % (base_url, job.uuid))

    assert response.code == 200

    data = json.loads(response.body)
    for key in ["test_data"]:
        assert key in data


@pytest.mark.gen_test
def test_progress_bad_uuid(app, http_client, base_url):
    with pytest.raises(HTTPError):
        yield http_client.fetch("%s/progress/nope" % base_url)


@pytest.mark.gen_test
def test_run_job(app, http_client, base_url):
    response = yield http_client.fetch(
        (
            "%s/runjob/%s" %
            (base_url, job_classes.GoodJob.JOB_TYPE)
        ),
        method="POST",
        body=json.dumps({})
    )

    assert response.code == 201
    assert "uuid" in json.loads(response.body)


@pytest.mark.gen_test
def test_run_job_bad_job_type(app, http_client, base_url):
    with pytest.raises(HTTPError):
        yield http_client.fetch(
            "%s/runjob/%s" % (base_url, "nope"),
            method="POST",
            body=json.dumps({})
        )
