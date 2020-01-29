#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
from zerog.jobs import BaseJob, make_key

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class JobRegistry(object):
    def __init__(self, datastore, queue, keepalive=None):
        """
        Maintain a registry of job classes which maps job type to job class.

        Use the type -> class mapping to instantiate jobs from class data

        Args:
            datastore: Datastore object for persisting jobs. Jobs need to
                       know this because they persist themselves

            queue: Job queue for sharing jobs with workers. Jobs need to know
                   this because they enqueue themselves
        """
        self.datastore = datastore
        self.queue = queue
        self.keepalive = keepalive

        self.registry = {}

    def add_classes(self, jobClasses):
        """
        Registers a list of job classes so they can be instantiated when
        they're loaded from the datastore.

        Args:
            jobClasses: List of job classes that this server needs to support.
                        Each class must have unique JOB_TYPE and SCHEMA class
                        attributes

        Returns:
            List of (jobClass, added, error) tuples, where added indicates if
            the class was successfully added, and error is the reason if not.
        """
        added = {}

        for jobClass in jobClasses:
            if not issubclass(jobClass, BaseJob):
                added[jobClass.__name__] = dict(
                    success=False, error="NotSubclass"
                )

            elif jobClass.JOB_TYPE == BaseJob.JOB_TYPE:
                added[jobClass.__name__] = dict(
                    success=False, error="NoJobType"
                )

            elif jobClass.SCHEMA == BaseJob.SCHEMA:
                added[jobClass.__name__] = dict(
                    success=False, error="NoSchema"
                )

            else:
                self.registry[jobClass.JOB_TYPE] = jobClass
                added[jobClass.__name__] = dict(
                    success=True, error=None
                )

        return added

    def get_registered_classes(self):
        return list(self.registry.values())

    def make_job(self, data, jobType=None):
        """
        Creates an instance of a job and validates that the data passed
        to instantiate it.

        Args:
            data: Data used to initialize the job's attributes

            jobType: jobClass.JOB_TYPE, or None if the jobType should be
                     gleaned from the input data

        Returns:
            job object
        """
        jobType = jobType or data.get('jobType')
        jobClass = self.registry.get(jobType)

        if jobClass:
            loaded = jobClass.SCHEMA().load(data)
            job = jobClass(
                self.datastore, self.queue, self.keepalive, **loaded
            )
            return job

        else:
            return None

    def get_job(self, uuid):
        """
        Creates an instance of a job from a job record saved in the
        datastore.

        Args:
            uuid: uuid of the job, which allows it to be uniquely
                  identified in the datastore
        """
        data, cas = self.datastore.read_with_cas(make_key(uuid))
        if data:
            data['cas'] = cas
            return self.make_job(data)
        else:
            return None
