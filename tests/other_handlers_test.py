import json
import pdb
import pytest

from zerog.handlers.uuid import ProgressHandler
from zerog.handlers.run_job import RunJobHandler
from zerog.jobs import BaseJob
from zerog.registry import find_subclasses

from tests import job_classes


# These tests vary from the "kwarg_handlers" tests in the endpoint patterns,
# as well as the way in which the arguments are passed.
#
# They need their own separate file because of limitations in the 'app'
# fixture

@pytest.fixture
def app(zerog_app):
    jobClasses = find_subclasses(BaseJob)
    handlers = [
        ("/progress", ProgressHandler),
        ("/runjob", RunJobHandler)
    ]
    app = zerog_app(jobClasses, handlers)
    return app


@pytest.mark.gen_test
def test_progress(app, http_client, base_url, make_test_job):
    job, registry = make_test_job(job_classes.GoodJob)
    job.save()
    response = yield http_client.fetch(
        "%s/progress?uuid=%s" % (base_url, job.uuid)
    )
    assert response.code == 200


@pytest.mark.gen_test
def test_run_job(app, http_client, base_url):
    response = yield http_client.fetch(
        "%s/runjob" % base_url,
        method="POST",
        body=json.dumps(dict(jobType=job_classes.GoodJob.JOB_TYPE))
    )

    assert response.code == 201
    assert "uuid" in json.loads(response.body)
