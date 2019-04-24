#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""
import datetime
import random
import time

import datastore_configs
import utils

from .error import ErrorSchema, make_error
from .event import EventSchema, make_event

from datastore import KeyExistsError, TemporaryFailError
from marshmallow import Schema, fields
from queue import queue_globals, sync_queue

import logging
log = logging.getLogger(__name__)

DEFAULT_JOB_LIFESPAN = 0
DEFAULT_TTR = 3600 * 24   # long because broken workers are killed

JOB_DOCUMENT_TYPE = "wf2_job"
BASE_JOB_TYPE = "wf2_base"


class BaseJobSchema(Schema):
    """
    Base Job Schema

    """
    documentType = fields.String()
    jobType = fields.String()
    schemaVersion = fields.Float()

    createdAt = fields.DateTime(format="iso")
    updatedAt = fields.DateTime(format="iso")
    cas = fields.Integer()

    uuid = fields.String()
    logId = fields.String()

    queueName = fields.String()
    queueKwargs = fields.Dict()
    queueJobId = fields.Integer()

    events = fields.List(fields.Nested(EventSchema))
    errors = fields.List(fields.Nested(ErrorSchema))

    completeness = fields.Float()
    resultCode = fields.Integer()
    resultString = fields.String()


class BaseJob(object):
    DOCUMENT_TYPE = JOB_DOCUMENT_TYPE
    JOB_TYPE = BASE_JOB_TYPE
    LIFESPAN = DEFAULT_JOB_LIFESPAN
    SCHEMA_VERSION = 1.0
    SCHEMA = BaseJobSchema
    QUEUE_NAME = ""

    def __init__(self, **kwargs):
        now = datetime.datetime.utcnow()

        self.documentType = kwargs.get('documentType', self.DOCUMENT_TYPE)
        self.jobType = kwargs.get('jobType', self.JOB_TYPE)
        self.schemaVersion = kwargs.get('schemaVersion', self.SCHEMA_VERSION)

        self.createdAt = kwargs.get('createdAt', now)
        self.updatedAt = kwargs.get('updatedAt', now)
        self.cas = kwargs.get('cas', 0)

        self.uuid = (kwargs.get('uuid') or utils.uuid_me())
        self.logId = kwargs.get(
            'logId',
            "%s_%s" % (self.JOB_TYPE, self.uuid)
        )

        self.queueKwargs = kwargs.get('queueKwargs', {})
        self.queueJobId = kwargs.get('queueJobId', 0)

        self.events = kwargs.get('events', [])
        self.errors = kwargs.get('errors', [])

        self.completeness = kwargs.get('completeness', 0)
        self.resultCode = kwargs.get('resultCode', queue_globals.NO_RESULT)
        self.resultString = kwargs.get('resultString', "")

    def dump(self):
        return self.SCHEMA().dump(self).data

    def dumps(self, **kwargs):
        return self.SCHEMA().dumps(self, **kwargs).data

    def __str__(self):
        return self.dumps(indent=4)

    def key(self):
        return make_key(self.DOCUMENT_TYPE, self.uuid)

    def save(self):
        """
        Saves job instance to the datastore. Current implementation uses
        Couchbase for the datastore.
        """
        self.updatedAt = datetime.datetime.utcnow()
        _, self.cas = datastore_configs.DATASTORE.set_with_cas(
            self.key(),
            self.dump(),
            cas=self.cas,
            ttl=self.LIFESPAN
        )

    def reload(self):
        """
        Reload job data by repulling from the datastore.

        This may be necessary if another python instance has updated this job
        in the datastore and the cas loaded here is out of date.
        """
        values, cas = datastore_configs.DATASTORE.read_with_cas(self.key())
        if values:
            values['cas'] = cas
            loaded = self.SCHEMA(strict=True).load(values).data
            self.__init__(**loaded)

    def lock(self, ttl=1):
        _, self.cas = datastore_configs.DATASTORE.lock(self.key(), ttl=ttl)

    def unlock(self):
        datastore_configs.DATASTORE.unlock(self.key(), self.cas)

    def record_change(self, func, *args, **kwargs):
        """
        Use func to update this job instance and save the updated instance to
        the datastore.

        In case of an error, reload the job from the datastore and retry.
        """
        for _ in range(10):
            try:
                func(*args, **kwargs)
                self.save()
                return True

            except KeyExistsError:
                log.info("%s: datastore collision - reloading." % self.logId)

            except TemporaryFailError:
                log.info("%s: locked - reloading." % self.logId)

            time.sleep(random.random() / 10)
            self.reload()

        log.error("%s: save failed - too many tries" % self.logId)
        return False

    def update_attrs(self, **kwargs):
        """
        Updates a job's attributes. Saves the update to the datastore.
        """
        def do_update_attrs():
            for attr, value in kwargs.items():
                setattr(self, attr, value)

        self.record_change(do_update_attrs)

    def record_event(self, msg, action=""):
        """
        Makes and records an event associated with this job.
        """
        event = make_event(action, msg)

        def do_record_event():
            self.events.append(event)

        self.record_change(do_record_event)

    def record_error(self, errorCode, msg, eventMsg=None):
        """
        Makes and records an error associated with this job. If there is a
        message, also makes and records an event.
        """
        if eventMsg:
            event = make_event(eventMsg, "")

        error = make_error(errorCode, msg)

        def do_record_error():
            if eventMsg:
                self.events.append(event)

            self.errors.append(error)

        self.record_change(do_record_error)

    def record_result(self, resultCode, resultString=""):
        """
        Record the result of a job.
        """
        self.update_attrs(
            resultCode=resultCode,
            resultString=resultString,
            completeness=1
        )

    def progress(self):
        """
        Returns a job's completeness, result, events, and errors.
        """
        return dict(
            completeness=self.completeness,
            result=self.resultCode,
            events=[e.dump() for e in self.events],
            errors=[e.dump() for e in self.errors]
        )

    def enqueue(self, **kwargs):
        """
        Add a job to its queue. The queue is defined by the subclass of
        BaseJob.
        """

        # if the job has not been added to the database yet, cas is 0
        if self.cas == 0:
            self.save()

        # thought: does this ever enqueue the job but then fail on the update?
        # what happens then?
        kwargs['ttr'] = kwargs.get('ttr', DEFAULT_TTR)
        queueJobId = sync_queue.QUEUE.put(
            self.QUEUE_NAME, self.uuid, **kwargs
        )

        self.update_attrs(queueKwargs=kwargs, queueJobId=queueJobId)

    def requeue_if_lost(self):
        # put job back in queue if it's supposed to be there
        # but isn't
        lost = (
            self.queueJobId and
            self.completeness < 1 and
            sync_queue.QUEUE.peek(self.queueJobId) is None
        )
        if lost:
            self.enqueue(**self.queueKwargs)

    def run(self):
        # override this method to execute the job. It is called by
        # the base worker once the job has been successfully loaded
        #
        # this method must return a tuple consisting of:
        #
        #   - the time (in epoch seconds) at which to resume
        #     consuming from the queue
        #
        #   - a boolean indicating whether the job should be
        #     re-queued for further processing
        #
        pass


def make_key(docType, uuid):
    """
    Makes a key based on docType and uuid to store job in the datastore.
    """
    return "%s_%s" % (docType, uuid)


def make_base_job(values={}, jobType=None):
    """

    """
    jobType = jobType or values.get('jobType')
    typeClasses = get_type_classes(jobType)

    if typeClasses:
        loaded = typeClasses['schema'](strict=True).load(values).data
        return typeClasses['model'](**loaded)
    else:
        return None


def get_base_job(uuid):
    """
    Loads an instance of a job based on the job information stored in the
    datastore with key based on uuid.
    """
    values, cas = datastore_configs.DATASTORE.read_with_cas(
        make_key(JOB_DOCUMENT_TYPE, uuid)
    )
    if values:
        values['cas'] = cas
        return make_base_job(values)

    else:
        return None


def get_type_classes(jobType):
    typeClasses = TYPE_CLASS_DICT.get(jobType)

    if typeClasses is None:
        log.error(
            "could not get typeClass for %s. Known typeClasses: %s" %
            (jobType, TYPE_CLASS_DICT.keys())
        )

    return typeClasses


def register_job(model):
    TYPE_CLASS_DICT[model.JOB_TYPE] = dict(schema=model.SCHEMA, model=model)


TYPE_CLASS_DICT = {}
