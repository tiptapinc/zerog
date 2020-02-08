#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import json
import pdb
import pytest
from tornado.httpclient import HTTPClientError

from zerog.handlers.progress import ProgressHandler
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
        ("/runjob/([^/]+)", RunJobHandler)
    ]
    app = zerog_app(jobClasses, handlers)
    return app


@pytest.mark.gen_test
def test_progress(app, http_client, base_url):
    job = app.registry.make_job(dict(), job_classes.GoodJob.JOB_TYPE)
    job.save()
    response = yield http_client.fetch("%s/progress/%s" % (base_url, job.uuid))

    assert response.code == 200

    progress = json.loads(response.body)
    for key in ["completeness", "result", "events", "errors", "warnings"]:
        assert key in progress


@pytest.mark.gen_test
def test_progress_bad_uuid(app, http_client, base_url):
    with pytest.raises(HTTPClientError):
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
    with pytest.raises(HTTPClientError):
        yield http_client.fetch(
            "%s/runjob/%s" % (base_url, "nope"),
            method="POST",
            body=json.dumps({})
        )
