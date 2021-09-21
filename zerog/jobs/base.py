#!/usr/bin/env python
# encoding: utf-8
# Copyright (c) 2017-2021 MotiveMetrics. All rights reserved.
"""
ZeroG BaseJobSchema and BaseJob class definitions
"""

#
# NOTES:
#   - make it possible to kill a job??
#

from abc import ABC, abstractmethod
import datetime
import psutil
import random
import time
import uuid

from marshmallow import Schema, fields

from .error import ErrorSchema, make_error
from .event import EventSchema, make_event
from .warning import WarningSchema, make_warning

import logging
log = logging.getLogger(__name__)

DEFAULT_TTR = 3600 * 24 * 30   # should never happen. When it does it's bad

# result codes
INTERNAL_ERROR = 500
NO_RESULT = -1

OVERRIDE_SIGNATURE = "zerog_job"


class ErrorContinue(Exception):
    pass


class ErrorFinish(Exception):
    pass


class WarningContinue(Exception):
    pass


class WarningFinish(Exception):
    pass


class BaseJobSchema(Schema):
    """
    BaseJob persisted attributes

    :var str documentType: used to create datastore key
    :var str jobType: used to get registered job schema
    :var float schemaVersion: used to update schemas

    :var datetime createdAt: time job was created
    :var datetime updatedAt: time job was last updated

    :var str cas: used to prevent job update collisions

    :var str uuid: unique job identifier
    :var str logId: job id to show in logs

    :var str queueName: name of queue for job
    :var dict queueKwargs: keyword args used to enqueue job
    :var int queueJobId: id of job in queue

    :var list events: list of logged events for job
    :var list errors: list of logged errors for job
    :var list warnings: list of logged warnings for job

    :var boolean running: True if job is currently running
    :var int errorCount: number of times job has had an exception
    :var float completeness: completion percentage 0.0 - 1.0
    :var float tickcount: record of completeness ticks
    :var float tickval: completeness increment per tick
    :var int resultCode: job resultCode, -1 if incomplete, 200 for success

    """
    documentType = fields.String()
    jobType = fields.String()
    schemaVersion = fields.Float()

    cas = fields.Integer()

    createdAt = fields.DateTime(format="iso")
    updatedAt = fields.DateTime(format="iso")

    uuid = fields.String()
    logId = fields.String()

    queueName = fields.String()
    queueKwargs = fields.Dict()
    queueJobId = fields.Integer()

    events = fields.List(fields.Nested(EventSchema))
    errors = fields.List(fields.Nested(ErrorSchema))
    warnings = fields.List(fields.Nested(WarningSchema))

    running = fields.Boolean()
    errorCount = fields.Integer()
    completeness = fields.Float()
    tickcount = fields.Float()
    tickval = fields.Float()
    resultCode = fields.Integer()


class BaseJob(ABC):
    """
    The base class for all ZeroG jobs.

    :cvar str DOCUMENT_TYPE: document type used to make the datastore key.
        NEVER override this attribute
    :cvar str SCHEMA_VERSION: version which can be used to manage schema
        changes. You MAY override this attribute
    :cvar class SCHEMA: the marshmallow schema used to serialize/deserialize
        this job. You MAY override this attribute to add fields to the base
        schema. The schema must be a subclass of BaseJobSchema
    :cvar str JOB_TYPE: a unique string identifying this type of job. You
        MUST override this attribute.
    :cvar int MAX_ERRORS: maximum number of error retries before the job
        fails. You MAY override this attribute.

    Subclasses MUST

        - call the base ``__init__()`` using ``super``
        - override the ``run()`` method
    """
    DOCUMENT_TYPE = "zerog_job"   # used to make datastore key
    SCHEMA_VERSION = "1.0"

    JOB_TYPE = OVERRIDE_SIGNATURE
    SCHEMA = OVERRIDE_SIGNATURE

    MAX_ERRORS = 3

    def __init__(self, datastore, queue, keepalive=None, **kwargs):
        """
        Initialize the job with deserialized data.

        Subclasses MUST override this method if they use a subclass of
        BaseJobSchema to add fields.

        If overriding this method, you MUST call the parent ``__init__()``
        using ``super``

        Example::

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

                self.myfield = kwargs.get('myfield')
        """
        self.datastore = datastore
        self.queue = queue
        self.keepalive = keepalive

        now = datetime.datetime.utcnow()

        self.documentType = kwargs.get('documentType', self.DOCUMENT_TYPE)
        self.jobType = kwargs.get('jobType', self.JOB_TYPE)
        self.schemaVersion = kwargs.get('schemaVersion', self.SCHEMA_VERSION)

        self.createdAt = kwargs.get('createdAt', now)
        self.updatedAt = kwargs.get('updatedAt', now)
        self.cas = kwargs.get('cas', 0)

        self.uuid = kwargs.get('uuid') or str(uuid.uuid4())
        self.logId = kwargs.get(
            'logId',
            "%s_%s" % (self.JOB_TYPE, self.uuid)
        )

        self.queueKwargs = kwargs.get('queueKwargs', {})
        self.queueJobId = kwargs.get('queueJobId', 0)

        self.events = kwargs.get('events', [])
        self.errors = kwargs.get('errors', [])
        self.warnings = kwargs.get('warnings', [])

        self.running = kwargs.get('running', False)
        self.errorCount = kwargs.get('errorCount', 0)
        self.completeness = kwargs.get('completeness', 0)
        self.tickcount = kwargs.get('tickcount', 0.0)
        self.tickval = kwargs.get('tickval', 0.001)
        self.resultCode = kwargs.get('resultCode', NO_RESULT)

    def dump(self):
        """
        Serialize the job according to the job schema.

        :returns: serialized job data
        :rtype: native Python data types
        """
        return self.SCHEMA().dump(self)

    def dumps(self, **kwargs):
        """
        Serialize the job according to the job schema.

        :returns: serialized job data
        :rtype: JSON-encoded string
        """
        return self.SCHEMA().dumps(self, **kwargs)

    def __str__(self):
        return self.dumps(indent=4)

    def key(self):
        """
        Constructs the datastore key for this job

        :returns: datastore key for this job
        :rtype: str
        """
        return make_key(self.uuid)

    def save(self):
        """
        Saves job instance to the datastore. Fails if job was updated in
        the datastore since this instance was last updated.

        :returns: ``None``
        """
        self.updatedAt = datetime.datetime.utcnow()
        _, self.cas = self.datastore.set_with_cas(
            self.key(),
            self.dump(),
            cas=self.cas
        )

    def reload(self):
        """
        Reload job data from the datastore and update this instance.

        This may be necessary if another python instance has updated this job
        in the datastore and the cas loaded here is out of date.

        :returns: ``None``
        """
        data, cas = self.datastore.read_with_cas(self.key())
        if data:
            data['cas'] = cas
            loaded = self.SCHEMA().load(data)
            self.__init__(self.datastore, self.queue, **loaded)

    def record_change(self, func, *args, **kwargs):
        # Use func to update this job instance and save the updated instance to
        # the datastore.

        # In case of an error, reload the job from the datastore and retry.

        # NOTE: How much is couchbase dependent?
        for _ in range(10):
            try:
                func(*args, **kwargs)
                self.save()
                return True

            except self.datastore.casException:
                log.info(
                    "pid {0}, uuid {1} collision - reloading.".format(
                        psutil.Process().pid, self.uuid
                    )
                )

            except self.datastore.lockedException:
                log.info(
                    "pid {0}, uuid {1} locked - reloading.".format(
                        psutil.Process().pid, self.uuid
                    )
                )

            time.sleep(random.random() / 10)
            self.reload()

        log.error(
            "pid {0}, uuid {1} save failed - too many collisions".format(
                psutil.Process().pid, self.uuid
            )
        )
        return False

    def update_attrs(self, **kwargs):
        """
        Updates a job's attributes. Only updates the attributes specified
        in the keyword arguments. Saves the update to the datastore.

        Example::

            self.update_attrs(queueJobId=10, running=True)

        :returns: ``None``
        """
        def do_update_attrs():
            for attr, value in kwargs.items():
                setattr(self, attr, value)

        self.record_change(do_update_attrs)

    def record_event(self, msg):
        """
        Records an event in the job's ``events`` list.

        :param str msg: the event message
        :returns: ``None``
        """
        event = make_event(msg)

        def do_record_event():
            self.events.append(event)

        self.record_change(do_record_event)

    def record_warning(self, msg):
        """
        Records a warning in the job's ``warnings`` list.

        :param str msg: the warning message
        :returns: ``None``
        """
        warning = make_warning(msg)

        def do_record_warning():
            self.warnings.append(warning)

        self.record_change(do_record_warning)

    def record_error(self, errorCode, msg, exception=None):
        """
        Records an error in the job's ``errors`` list.

        The ``exception`` keyword argument is passed so that a job can
        override this method to add extra error handling for specific
        exceptions.

        :param int errorCode: an error code associated with the error
        :param str msg: the error message
        :param object exception: the exception that caused the error if
            the error is the result of an unhandled exception
        :returns: ``None``
        """
        error = make_error(errorCode, msg)

        def do_record_error():
            self.errors.append(error)

        self.record_change(do_record_error)
        self.update_attrs(errorCount=self.errorCount + 1)

    def record_result(self, resultCode):
        """
        Record the result of a job. This method is called by the base worker
        when a job completes, so it does not need to be explicitly called in
        most cases.
        """
        self.update_attrs(
            resultCode=resultCode,
            completeness=1
        )

    def keep_alive(self):
        if self.keepalive and callable(self.keepalive):
            self.keepalive()

    def job_log_info(self, msg):
        """
        Records an event in the job's ``events`` list and logs it using
        the current Python logger.

        :param str msg: the event message
        :returns: ``None``
        """
        log.info(msg)
        self.record_event(msg)

    def job_log_warning(self, msg):
        """
        Records a warning in the job's ``warnings`` list and logs it using
        the current Python logger.

        :param str msg: the warning message
        :returns: ``None``
        """
        log.warning(msg)
        self.record_warning(msg)

    def job_log_error(self, errorCode, msg, exception=None):
        """
        Records an error in the job's ``errors`` list and logs it using
        the current Python logger.

        :param int errorCode: an error code associated with the error
        :param str msg: the error message
        :param object exception: the exception that caused the error if
            the error is the result of an unhandled exception
        :returns: ``None``
        """
        log.error(msg)
        self.record_error(errorCode, msg, exception=exception)

    def raise_warning_continue(self, resultCode, msg):
        """
        Interrupts job execution and records a warning. Job may continue
        after being requeued

        :param str msg: the warning message
        """
        self.job_log_warning(str(msg))
        raise WarningContinue

    def raise_warning_finish(self, resultCode, msg):
        """
        Interrupts job execution, records a warning, and terminates the job.

        :param str msg: the warning message
        """
        self.job_log_warning(str(msg))
        self.record_result(resultCode)
        raise WarningFinish

    def raise_error_continue(self, errorCode, msg):
        """
        Interrupts job execution and records an error. Job may continue
        after being requeued

        :param int errorCode: an error code associated with the error
        :param str msg: the error message
        """
        self.job_log_error(errorCode, str(msg))
        raise ErrorContinue

    def raise_error_finish(self, errorCode, msg):
        """
        Interrupts job execution, records an error, and terminates the job.

        :param int errorCode: an error code associated with the error
        :param str msg: the error message
        """
        self.job_log_error(errorCode, str(msg))
        self.record_result(errorCode)
        raise ErrorFinish

    def set_completeness(self, completeness):
        """
        Sets the absolute value of the job's completeness. Clamps
        value to a range of 0.0 to 1.0

        :param float completeness: absolute completeness value
        :returns: ``None``
        """
        self.keep_alive()
        setval = clamp(completeness, 0.0, 1.0)

        if completeness < 0 or completeness > 1:
            log.warning(
                "completeness %d out of range. Clamping to %d" %
                (completeness, setval)
            )

        self.update_attrs(completeness=setval, tickcount=self.tickcount)

    def add_to_completeness(self, delta):
        """
        Increment the job's completeness. Adds any unrecorded ticks.
        Resulting completeness will be clamped to a range of 0.0 to 1.0.

        :param float delta: amount by which to increment completeness
        :returns: ``None``
        """
        self.set_completeness(self.completeness + delta + self.tickcount)

    def set_tick_value(self, tickval):
        """
        Sets the amount the job's completeness will be incremented
        by a call to the tick method

        :param float tickval: amount to increment completeness for each tick
        :returns: ``None``
        """
        self.update_attrs(tickval=tickval)

    def tick(self):
        """
        Accumulates the job's tickcount. Adds tickcount to completeness
        when it is >= 0.01

        :returns: ``None``
        """
        self.tickcount += self.tickval

        if self.tickcount >= 0.01:
            self.add_to_completeness(0)
            self.tickcount = 0

    def enqueue(self, **kwargs):
        """
        Add a job to its queue.

        Sets the job's queueJobId if enqueueing is successful. Sets it to -1
        if enqueueing fails. 

        :params dict kwargs: keyword arguments that will be passed to the
            queueing client
        :returns: ``None``
        """

        # if the job has not been added to the database yet, cas is 0
        if self.cas == 0:
            self.save()

        kwargs['ttr'] = kwargs.get('ttr', DEFAULT_TTR)
        queueJobId = self.queue.put(self.uuid, **kwargs)

        # if we don't get a valid job id back from the attempt to enqueue,
        # set queueJobId to -1 so we can later see that there was a problem
        if not queueJobId:
            log.warning(f"{self.jobType} {self.uuid} enqueue failed")
            queueJobId = -1

        self.update_attrs(queueKwargs=kwargs, queueJobId=queueJobId)

    def progress(self):
        """
        Returns a job's completeness and result.

        :returns: current values of completeness & resultCode
        :rtype: dict
        """
        return dict(
            completeness=self.completeness,
            result=self.resultCode
        )

    def info(self):
        """
        Returns a job's completeness, result, events, warnings, and errors.

        :returns: current values of completeness, resultCode, events,
            warnings, and errors
        :rtype: dict
        """
        return dict(
            completeness=self.completeness,
            result=self.resultCode,
            events=[e.dump() for e in self.events],
            errors=[e.dump() for e in self.errors],
            warnings=[w.dump() for w in self.warnings]
        )

    def get_data(self):
        """
        Returns result data for this job.

        Override this method if the job needs to return data once it is
        complete.

        :returns: output data
        :rtype: dict
        """
        return {}

    def continue_running(self):
        """
        called by the worker after a job is interrupted by an exception to
        determine if the job should be requeued to continue running

        Default is to terminate after self.MAX_ERRORs errors have been
        recorded.

        Override this method as needed for more complex error handling

        :returns: NO_RESULT (-1) if the job should continue. INTERNAL_ERROR
            (500) if the job should terminate
        :rtype: int
        """
        if self.errorCount >= self.MAX_ERRORS:
            return INTERNAL_ERROR

        return NO_RESULT

    @abstractmethod
    def run(self):
        """
        This method MUST be overridden.

        This method executes the job. It is called by the base worker once
        the job has been successfully loaded
        
        :returns: resultCode for the job. Return NO_RESULT if job needs
            to be requeued for further processing. Otherwise use HTTP
            resultCodes (200s for success, etc.)
        """
        pass


def make_key(uuid):
    """
    Makes a unique datastore key for a job.

    :param str uuid: uuid of the job
    :returns: datastore key
    :rtype: str
    """
    return "%s_%s" % (BaseJob.DOCUMENT_TYPE, uuid)


def clamp(value, minval, maxval):
    return (max(min(maxval, value), minval))
