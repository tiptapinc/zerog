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

import zerog
from zerog.mgmt import MgmtChannel, make_msg, make_worker_id

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
        name: service name for this zerog server
        makeDatastore: function to create a Datastore object that can be
                       used to persist & retrieve jobs.

        makeQueue: function to create Queue objects for posting jobs & for
                   inter-server communications

        jobClasses: List of job classes (derived from BaseClass) that
                    this ZeroG instance will support. Additional job
                    classes can be added later with the add_to_registry
                    method

        thisHost: host IP address or hostname

        handlers: request handlers to be passed to parent __init__ method

        **kwargs: passed to parent __init__method
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
        self.pid = psutil.Process().pid
        log.info("initializing ZeroG server {0}".format(self.pid))

        self.name = name
        self.thisHost = thisHost
        self.workerId = make_worker_id("zerog", thisHost, name, self.pid)

        self.datastore = makeDatastore()
        self.jobQueue = makeQueue("{0}_jobs".format(self.name))
        self.updatesChannel = MgmtChannel(
            makeQueue(zerog.UPDATES_CHANNEL_NAME)
        )
        self.ctrlChannel = MgmtChannel(makeQueue(self.workerId))

        self.registry = zerog.JobRegistry()
        self.registry.add_classes(jobClasses)

        self.state = "polling"
        self.workerStatus = ""
        self.runningJobUuid = ""

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
        log.info(f"server {self.pid} exiting")
        self.kill_worker()

    def make_worker(self, makeDatastore, makeQueue):
        log.info(f"server {self.pid} creating worker")
        self.parentConn, self.childConn = multiprocessing.Pipe()
        self.worker = zerog.BaseWorker(
            self.name, makeDatastore, makeQueue, self.registry, self.childConn
        )
        self.start_worker()

        tornado.ioloop.IOLoop.instance().call_later(
            0, self.poll
        )

    def start_worker(self):
        self.proc = multiprocessing.Process(target=self.worker.run)
        self.proc.daemon = True
        self.proc.start()
        log.info(f"server {self.pid} started worker {self.proc.pid}")

    def kill_worker(self, killJob=False):
        self.do_poll()

        log.info(
            f"server {self.pid} killing worker {self.proc.pid}, "
            f"activeJob: {self.runningJobUuid}"
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
        log.info(f"server {self.pid} worker {self.proc.pid} stop polling")
        self.parentConn.send("drain")
        self.state = "draining"

    def process_worker_message(self, msg):
        msgType = msg.get('type')
        if not msgType:
            log.error(f"server {self.pid} worker message has no type\n{msg}")
            return

        if msgType == 'runningJobUuid':
            newRunningJobUuid = msg['value']
            if newRunningJobUuid:
                kwargs = dict(action="start", uuid=newRunningJobUuid)
                self.state = "runningJob"
            else:
                kwargs = dict(action="end", uuid=self.runningJobUuid)
                if self.state != "draining":
                    self.state = "polling"

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
                    f"server {self.pid} can't parse worker message\n{msg}\n{e}"
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
                log.info(f"server {self.pid}, worker {self.pid} is zombie.")
                if self.runningJobUuid:
                    job = self.get_job(self.runningJobUuid)
                    if job:
                        job.job_log_error(
                            zerog.jobs.INTERNAL_ERROR,
                            "worker failed - possibly out of memory\n"
                        )
                    else:
                        log.error(
                            f"server {self.pid} can't load last running job"
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

    def do_control_queue_poll(self):
        msg = self.ctrlChannel.get_msg()
        if msg:
            if msg.msgtype == "requestInfo":
                infomsg = make_msg(
                    "info",
                    workerId=self.workerId,
                    state=self.state,
                    uuid=self.runningJobUuid,
                    mem=dict(psutil.virtual_memory()._asdict())
                )
                self.updatesChannel.send_msg(infomsg)

            elif msg.msgtype == "drain":
                self.drain()

            elif msg.msgtype == "killJob":
                uuid = self.runningJobUuid
                if uuid and uuid == msg.uuid:
                    self.kill_worker(killJob=True)
                    self.start_worker()
