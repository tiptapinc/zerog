#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""
import atexit
import multiprocessing
import json
import os
import psutil
import signal
import tornado.web
import tornado.ioloop

import zerog.registry
import zerog.workers

import logging
log = logging.getLogger(__name__)

HANDLERS = []
POLL_INTERVAL = 2
POLL_JITTER = 0.1


class Server(tornado.web.Application):
    """
    Initialize a ZeroG Server, which is a subclass of Tornado's
    Application class

    Args:
        name: name for this zerog server
        makeDatastore: function to create a Datastore object that can be
                       used to persist & retrieve jobs.

        makeQueue: function to create Queue objects for posting jobs & for
                   inter-server communications

        ctrlQueue: Queue for sharing control information among servers

        jobClasses: List of job classes (derived from BaseClass) that
                    this ZeroG instance will support. Additional job
                    classes can be added later with the add_to_registry
                    method

        *args: passed to parent __init__ method

        **kwargs: passed to parent __init__method
    """
    def __init__(
        self, name, makeDatastore, makeQueue, jobClasses, handlers=[], **kwargs
    ):
        self.workerStatus = ""
        self.pid = psutil.Process().pid
        self.runningJobUuid = ""

        log.info("initializing ZeroG server {0}".format(self.pid))

        self.name = name
        self.datastore = makeDatastore()
        self.jobQueue = makeQueue("{0}_jobs".format(self.name))
        self.ctrlQueue = makeQueue("{0}_ctrl".format(self.name))

        self.registry = zerog.registry.JobRegistry()
        self.registry.add_classes(jobClasses)

        self.make_worker(makeDatastore, makeQueue)
        atexit.register(self.exit_handler)

        handlers += HANDLERS
        super(Server, self).__init__(handlers, **kwargs)

    def make_job(self, data, jobType):
        return self.registry.make_job(
            data, self.datastore, self.jobQueue, None, jobType=jobType
        )

    def get_job(self, uuid):
        return self.registry.get_job(
            uuid, self.datastore, self.jobQueue, None
        )

    def exit_handler(self):
        """
        Makes sure worker dies on exit
        """
        log.info("server {0} exiting".format(self.pid))
        self.kill_worker()

    def make_worker(self, makeDatastore, makeQueue):
        log.info(
            "server {0} creating worker".format(
                self.pid
            )
        )
        self.parentConn, self.childConn = multiprocessing.Pipe()
        self.worker = zerog.workers.BaseWorker(
            self.name, makeDatastore, makeQueue, self.registry, self.childConn
        )
        self.start_worker()

        tornado.ioloop.IOLoop.instance().call_later(
            0, self.worker_poll
        )

    def start_worker(self):
        self.proc = multiprocessing.Process(target=self.worker.run)
        self.proc.daemon = True
        self.proc.start()

        log.info(
            "server {0} started worker {1}".format(
                self.pid, self.proc.pid
            )
        )

    def kill_worker(self):
        self.do_worker_poll()

        if self.runningJobUuid:
            job = self.get_job(self.runningJobUuid)
            job.record_event("System restart")

        log.info(
            "server {0} killing worker {1}, activeJob: {2}".format(
                self.pid, self.proc.pid, self.runningJobUuid
            )
        )
        self.proc.kill()

    def stop_worker_polling(self):
        log.info(
            "server {0} worker {1} stop polling".format(
                self.pid, self.proc.pid
            )
        )
        self.parentConn.send("stop polling")

    def start_worker_polling(self):
        log.info(
            "server {0} worker {1} start polling".format(
                self.pid, self.proc.pid
            )
        )
        self.parentConn.send("start polling")

    def stop_worker(self):
        self.parentConn.send("die")
        self.proc.join(0)
        log.info(
            "server {0} worker {1} stop requested. exitcode: {2}".format(
                self.pid, self.proc.pid, self.proc.exitcode
            )
        )

    def process_worker_message(self, msg):
        msgType = msg.get('type')
        if not msgType:
            log.error(
                "server {0} worker message has no type\n{1}".format(
                    self.pid, msg
                )
            )
            return

        if msgType == 'runningJobUuid':
            self.runningJobUuid = msg['value']

    def worker_poll(self):
        self.do_worker_poll()
        tornado.ioloop.IOLoop.instance().call_later(
            POLL_INTERVAL, self.worker_poll
        )

    def do_worker_poll(self):
        while self.parentConn.poll() is True:
            text = self.parentConn.recv()
            try:
                msg = json.loads(text)
            except (TypeError, json.decoder.JSONDecodeError) as e:
                log.error(
                    "server {0} can't parse worker message\n{1}\n{2}".format(
                        self.pid, msg, e
                    )
                )
            else:
                self.process_worker_message(msg)

        try:
            workerStatus = psutil.Process(self.proc.pid).status()
        except psutil.NoSuchProcess:
            workerStatus = "NoSuchProcess"

        if workerStatus != self.workerStatus:
            if workerStatus == psutil.STATUS_ZOMBIE:
                self.proc.join(0)
                log.info(
                    "server {0}, worker {1} is zombie.".format(
                        self.pid, self.proc.pid
                    )
                )
                if self.runningJobUuid:
                    job = self.get_job(self.runningJobUuid)
                    if job:
                        job.job_log_error(
                            zerog.jobs.INTERNAL_ERROR,
                            "worker failed - possibly out of memory"
                        )
                    else:
                        log.error(
                            "server {0} can't load last running job".format(
                                self.pid
                            )
                        )
                restart = True

            elif workerStatus == "NoSuchProcess":
                log.info(
                    "server {0}, worker {1} is gone".format(
                        self.pid, self.proc.pid
                    )
                )
                restart = True
            else:
                restart = False

            if restart:
                log.info("server {0} restarting worker".format(self.pid))
                self.start_worker()

            self.workerStatus = workerStatus
