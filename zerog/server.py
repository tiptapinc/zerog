#!/usr/bin/env python
# encoding: utf-8
# Copyright (c) 2017-2021 MotiveMetrics. All rights reserved.
"""
ZeroG Server class definition
"""

import atexit
import multiprocessing
import json
import psutil
import time
import tornado.web
import tornado.ioloop

import zerog
from zerog.mgmt import MgmtChannel, make_msg, make_worker_id

import logging
log = logging.getLogger(__name__)

HANDLERS = []
POLL_INTERVAL = 2
POLL_JITTER = 0.1

ACTIVE_IDLE = "activeIdle"
ACTIVE_RUNNING = "activeRunning"
DRAINING_IDLE = "drainingIdle"
DRAINING_RUNNING = "drainingRunning"
DRAINING_DOWN = "drainingDown"


class Server(tornado.web.Application):
    """
    Base ZeroG server class
    """
    def __init__(
        self,
        name,
        makeDatastore,
        makeQueue,
        jobClasses,
        handlers=[],
        thisHost='localhost',
        **kwargs
    ):
        """
        :param name: service name for this zerog server
        :type name: str

        :param makeDatastore: function to create a Datastore object that can
            be used to persist & retrieve jobs.
        :type makeDatastore: function

        :param makeQueue: function to create Queue objects for posting jobs
            & for inter-server communications
        :type makeQueue: function

        :param jobClasses: List of job classes (derived from BaseClass) that
            this ZeroG instance will support. Additional job classes can be
            added later with the add_to_registry method
        :type jobClasses: list

        :param thisHost: host IP address or hostname
        :type thisHost: str

        :param handlers: request handlers to be passed to parent ``__init__``
            method
        :type handlers: list of tuples

        :param `**kwargs`: passed to parent ``__init__`` method
        """
        self.pid = psutil.Process().pid

        self.name = name
        self.thisHost = thisHost
        self.workerId = make_worker_id("zerog", thisHost, name, self.pid)
        log.info(f"workerId: {self.workerId} | initializing")

        self.datastore = makeDatastore()
        self.jobQueue = makeQueue("{0}_jobs".format(self.name))
        self.updatesChannel = MgmtChannel(
            makeQueue(zerog.UPDATES_CHANNEL_NAME)
        )
        self.ctrlChannel = MgmtChannel(makeQueue(self.workerId))

        self.registry = zerog.JobRegistry()
        self.registry.add_classes(jobClasses)

        self.state = ACTIVE_IDLE
        self.retiring = False
        self.workerStatus = ""
        self.runningJobUuid = ""

        self.make_worker(makeDatastore, makeQueue)
        atexit.register(self.exit_handler)

        handlers += HANDLERS
        super(Server, self).__init__(handlers, **kwargs)

    def make_job(self, data, jobType):
        """
        Instantiate a job from deserialized job attribute data

        :param data: deserialized job attributes data
        :type data: dict

        :param str jobType: ``jobType`` string -- must map to a ``jobType``
            registered with this Server's registry
        """
        return self.registry.make_job(
            data, self.datastore, self.jobQueue, None, jobType=jobType
        )

    def get_job(self, uuid):
        """
        Retrieve and instantiate a job that has been persisted in the datastore

        :param str uuid: UUID of the job to be retrieved and instantiated
        """
        return self.registry.get_job(
            uuid, self.datastore, self.jobQueue, None
        )

    def exit_handler(self):
        """
        Should be called on system exit to ensure that the system exit can be
        logged in any actively running job 
        """
        log.info(f"{self.name}:{self.pid} | exiting")
        self.kill_worker()

    def make_worker(self, makeDatastore, makeQueue):
        log.info(f"{self.name}:{self.pid} | creating worker")
        self.parentConn, self.childConn = multiprocessing.Pipe()
        self.worker = zerog.BaseWorker(
            self.name, makeDatastore, makeQueue, self.registry, self.childConn
        )
        self.start_worker()
        self.callback = tornado.ioloop.PeriodicCallback(
            self.do_poll, POLL_INTERVAL * 1000
        )
        self.callback.start()

    def start_worker(self):
        self.proc = multiprocessing.Process(target=self.worker.run)
        self.proc.start()
        self.state = ACTIVE_IDLE
        log.info(f"{self.name}:{self.pid}:{self.proc.pid} | started worker")

    def kill_worker(self, killJob=False):
        self.do_poll()

        log.info(
            f"{self.name}:{self.pid}:{self.proc.pid} | "
            f"killing worker | activeJob: {self.runningJobUuid}"
        )
        self.proc.kill()

        # runningJobUuid is still set to the last job because we haven't
        # run do_poll() yet
        if self.runningJobUuid:
            job = self.get_job(self.runningJobUuid)
            if killJob:
                self.runningJobUuid = ""
                job.record_error(410, msg="Killed by user")
                job.record_result(410)  # 'Gone' is best fit error code
                self.jobQueue.delete(job.queueJobId)
            else:
                job.record_event("System restart")

    def drain(self):
        if self.state == ACTIVE_IDLE:
            self.state = DRAINING_IDLE
            log.info(
                f"{self.name}:{self.pid}:{self.proc.pid} | drain - no job"
            )
            self.parentConn.send("drain")

        elif self.state == ACTIVE_RUNNING:
            self.state = DRAINING_RUNNING
            log.info(
                f"{self.name}:{self.pid}:{self.proc.pid} | "
                f"drain - finish job {self.runningJobUuid}"
            )
        else:
            log.info(
                f"{self.name}:{self.pid}:{self.proc.pid} | "
                f"drain - state {self.state}")

    def undrain(self):
        if self.retiring:
            return

        self.parentConn.send("undrain")
        if self.state in [DRAINING_IDLE, DRAINING_DOWN]:
            self.state = ACTIVE_IDLE

        elif self.state == DRAINING_RUNNING:
            self.state = ACTIVE_RUNNING

    def process_worker_message(self, msg):
        msgType = msg.get('type')
        if not msgType:
            log.error(
                f"{self.name}:{self.pid}:{self.proc.pid} | "
                f"worker message has no type\n{msg}"
            )
            return

        if msgType == 'runningJobUuid':
            newRunningJobUuid = msg['value']
            if newRunningJobUuid:
                kwargs = dict(action="start", uuid=newRunningJobUuid)
                if self.state in [ACTIVE_IDLE, ACTIVE_RUNNING]:
                    self.state = ACTIVE_RUNNING
                else:
                    self.state = DRAINING_RUNNING
            else:
                kwargs = dict(action="end", uuid=self.runningJobUuid)

            self.runningJobUuid = newRunningJobUuid
            kwargs['workerId'] = self.workerId
            msg = make_msg("job", **kwargs)
            self.updatesChannel.send_msg(msg)

    def poll(self):
        self.do_poll()
        tornado.ioloop.IOLoop.instance().call_later(
            POLL_INTERVAL, self.poll
        )

    def do_poll(self):
        self.do_worker_poll()
        self.do_control_queue_poll()

    def do_worker_poll(self):
        while self.parentConn.poll() is True:
            text = self.parentConn.recv()
            try:
                msg = json.loads(text)
            except (TypeError, json.decoder.JSONDecodeError) as e:
                log.error(
                    f"{self.name}:{self.pid}:{self.proc.pid} | "
                    f"can't parse worker message\n{msg}\n{e}"
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
                exitcode = self.proc.exitcode
                if exitcode != 0:
                    log.info(
                        f"{self.name}:{self.pid}:{self.proc.pid} | "
                        f"killed. exitcode: {exitcode}"
                    )
                self.runningJobUuid = ""
                restart = True

            elif workerStatus == "NoSuchProcess":
                log.info(
                    f"{self.name}:{self.pid}:{self.proc.pid} | "
                    "no such process"
                )
                restart = True
            else:
                restart = False

            log.info(
                f"{self.name}:{self.pid}:{self.proc.pid} | "
                f"workerStatus: {workerStatus}, state: {self.state}"
            )
            if restart:
                if self.state in [ACTIVE_IDLE, ACTIVE_RUNNING]:
                    log.info(
                        f"{self.name}:{self.pid}:{self.proc.pid} | "
                        "restarting worker"
                    )
                    self.state = ACTIVE_IDLE
                    self.start_worker()
                else:
                    self.state = DRAINING_DOWN

            self.workerStatus = workerStatus

    def do_control_queue_poll(self):
        while True:
            msg = self.ctrlChannel.get_msg()
            if not msg:
                break
            else:
                if msg.msgtype == "requestInfo":
                    try:
                        p = psutil.Process(self.pid)
                        used = p.memory_full_info().uss
                        for kid in p.children(recursive=True):
                            used += kid.memory_full_info().uss
                    except psutil.NoSuchProcess:
                        used = 0

                    mem = dict(
                        available=psutil.virtual_memory().available,
                        used=used
                    )
                    infomsg = make_msg(
                        "info",
                        workerId=self.workerId,
                        state=self.state,
                        retiring=self.retiring,
                        uuid=self.runningJobUuid,
                        mem=mem,
                    )
                    self.updatesChannel.send_msg(infomsg)

                elif msg.msgtype == "drain":
                    self.drain()

                elif msg.msgtype == "undrain":
                    self.undrain()

                elif msg.msgtype == "retire":
                    self.retiring = True
                    self.drain()

                elif msg.msgtype == "killJob":
                    uuid = self.runningJobUuid
                    if uuid and uuid == msg.uuid:
                        self.kill_worker(killJob=True)
                        self.start_worker()
