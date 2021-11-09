import datetime
import pdb
import pytest
import os
import signal
import subprocess
import time

from tests.job_classes import SleepJob
from zerog.mgmt import (
    MgmtChannel, make_msg, make_worker_id, parse_worker_id, WorkerManager
)
from zerog.mgmt.messages import InfoMsg


@pytest.fixture
def run_server_app():
    proc = None

    def _func(extraArgs=[]):
        nonlocal proc
        args = [
            "gunicorn",
            "--workers=1",
            "--bind=0.0.0.0:8099"
        ]
        args += extraArgs
        args += [
            "-k tornado",
            "tests.server:make_app()"
        ]
        proc = subprocess.Popen(args)
        return proc

    yield _func
    # cleanup code
    os.kill(proc.pid, signal.SIGQUIT)
    time.sleep(1)


@pytest.fixture
def server_app(zerog_app, clear_queue):
    app = None

    def _func(jobClasses=[]):
        nonlocal app
        app = zerog_app(jobClasses, [])
        return app

    yield _func
    # cleanup code
    app.kill_worker()
    clear_queue(app.ctrlChannel.queue)


@pytest.fixture
def make_sleep_job(job_registry, datastore, jobs_queue, clear_queue):
    def _func(delay):
        registry = job_registry(SleepJob)
        job = registry.make_job(
            dict(delay=delay),
            datastore,
            jobs_queue,
            jobType=SleepJob.JOB_TYPE
        )
        job.save()
        clear_queue(job.queue)
        job.enqueue()
        return job

    return _func


def wait_until_running(j, timeout):
    startTime = time.time()
    while True:
        time.sleep(0.25)
        j.reload()
        if j.running:
            return True

        if time.time() - startTime > timeout:
            return False


@pytest.fixture
def make_channel(make_queue):
    def _func(queueName):
        queue = make_queue(queueName)
        channel = MgmtChannel(queue)
        return channel

    return _func


def test_list_all_queues(make_channel):
    queueName = "test_list_all_queues"
    channel = make_channel(queueName)
    allQueues = channel.list_all_queues()

    assert queueName in allQueues


def test_detach_channel(make_channel):
    channel = make_channel("test_detach")

    allQueues = channel.list_all_queues()
    assert "test_detach" in allQueues

    channel.detach()
    allQueues = channel.list_all_queues()
    assert "test_detach" not in allQueues


def test_make_msg(make_channel):
    channel = make_channel("test")
    msg = channel.make_msg("info", workerId="whatever", state="draining")

    assert isinstance(msg, InfoMsg)
    assert msg.msgtype == "info"
    assert msg.workerId == "whatever"
    assert msg.state == "draining"


def test_send_and_get_message(make_channel, clear_queue):
    channelName = "updates"
    workerChannel = make_channel(channelName)
    managerChannel = make_channel(channelName)
    clear_queue(workerChannel.queue)

    sendmsg = workerChannel.make_msg(
        "info", workerId="whatever", state="polling"
    )
    workerChannel.send_msg(sendmsg)

    getmsg = managerChannel.get_msg()
    assert sendmsg.dump() == getmsg.dump()


def test_make_and_parse_worker_id():
    testhost = "localhost"
    testname = "Frank"
    testpid = 55

    workerId = make_worker_id(
        "zerog", testhost, testname, testpid
    )
    assert isinstance(workerId, str)

    parsed = parse_worker_id(workerId)
    assert parsed is not None
    assert parsed['workerType'] == "zerog"
    assert parsed['host'] == testhost
    assert parsed['serviceName'] == testname
    assert parsed['pid'] == testpid


def test_server_makes_queues(server_app, make_channel):
    app = server_app()
    workerId = make_worker_id(
        "zerog", app.thisHost, app.name, app.pid
    )
    channel = make_channel("updates")
    allQueues = channel.list_all_queues()
    assert workerId in allQueues


def test_job_msgs(server_app, make_sleep_job, make_channel, clear_queue):
    channel = make_channel("updates")
    clear_queue(channel.queue)
    app = server_app([SleepJob])

    # just creating the server should start its worker, so it should
    # pick up and run jobs
    sleeptime = 5
    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 30)

    time.sleep(sleeptime + 1)
    j.reload()
    assert j.resultCode == 200

    # manually run the server app's main event loop
    app.do_poll()

    msg0 = channel.get_msg()
    assert msg0 is not None
    assert msg0.msgtype == "job"
    assert msg0.uuid == j.uuid
    assert msg0.action == "start"

    msg1 = channel.get_msg()
    assert msg1 is not None
    assert msg1.msgtype == "job"
    assert msg1.uuid == j.uuid
    assert msg1.action == "end"


def test_request_info_msg(server_app, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app()

    workerId = make_worker_id(
        "zerog", app.thisHost, app.name, app.pid
    )
    ctrlchannel = make_channel(workerId)
    msg = make_msg("requestInfo")
    ctrlchannel.send_msg(msg)

    app.do_poll()
    infomsg = updateschannel.get_msg()

    assert infomsg is not None
    assert infomsg.msgtype == "info"
    assert infomsg.workerId == workerId
    assert infomsg.state == "activeIdle"


def test_get_running_job_uuid(
    server_app, make_sleep_job, make_channel, clear_queue
):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerId = make_worker_id(
        "zerog", app.thisHost, app.name, app.pid
    )
    ctrlchannel = make_channel(workerId)

    sleeptime = 5
    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 30)

    app.do_poll()
    clear_queue(updateschannel.queue)
    msg = make_msg("requestInfo")
    ctrlchannel.send_msg(msg)

    app.do_poll()
    infomsg = updateschannel.get_msg()

    assert infomsg is not None
    assert infomsg.msgtype == "info"
    assert infomsg.workerId == workerId
    assert infomsg.state == "activeRunning"
    assert infomsg.uuid == j.uuid

    time.sleep(sleeptime + 1)

    j.reload()
    assert j.resultCode == 200

    app.do_poll()
    clear_queue(updateschannel.queue)
    msg = make_msg("requestInfo")
    ctrlchannel.send_msg(msg)

    app.do_poll()
    infomsg = updateschannel.get_msg()

    assert infomsg is not None
    assert infomsg.msgtype == "info"
    assert infomsg.workerId == workerId
    assert infomsg.state == "activeIdle"


def test_drain(server_app, make_sleep_job, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerId = make_worker_id(
        "zerog", app.thisHost, app.name, app.pid
    )
    ctrlchannel = make_channel(workerId)

    sleeptime = 5
    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 30)

    msg = make_msg("drain")
    ctrlchannel.send_msg(msg)
    app.do_poll()
    assert app.state == "drainingRunning"

    clear_queue(updateschannel.queue)
    msg = make_msg("requestInfo")
    ctrlchannel.send_msg(msg)

    app.do_poll()
    infomsg = updateschannel.get_msg()
    assert infomsg is not None
    assert infomsg.msgtype == "info"
    assert infomsg.workerId == workerId
    assert infomsg.state == "drainingRunning"
    assert infomsg.retiring is False

    time.sleep(sleeptime + 1)

    j.reload()
    assert j.resultCode == 200
    app.do_poll()

    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 10) is False


def test_kill_job(server_app, make_sleep_job, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerId = make_worker_id(
        "zerog", app.thisHost, app.name, app.pid
    )
    ctrlchannel = make_channel(workerId)

    sleeptime = 5
    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 30)

    msg = make_msg("killJob", uuid=j.uuid)
    ctrlchannel.send_msg(msg)
    app.do_poll()

    time.sleep(sleeptime + 1)
    j.reload()
    assert j.resultCode == 410
    assert "Killed by user" in j.errors[0].msg


def test_un_drain(server_app, make_sleep_job, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerId = make_worker_id(
        "zerog", app.thisHost, app.name, app.pid
    )
    ctrlchannel = make_channel(workerId)

    sleeptime = 5
    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 30)

    msg = make_msg("drain")
    ctrlchannel.send_msg(msg)
    app.do_poll()
    assert app.state == "drainingRunning"

    clear_queue(updateschannel.queue)
    msg = make_msg("undrain")
    ctrlchannel.send_msg(msg)
    app.do_poll()
    assert app.state == "activeRunning"

    clear_queue(updateschannel.queue)
    msg = make_msg("requestInfo")
    ctrlchannel.send_msg(msg)

    app.do_poll()
    infomsg = updateschannel.get_msg()
    assert infomsg is not None
    assert infomsg.msgtype == "info"
    assert infomsg.workerId == workerId
    assert infomsg.state == "activeRunning"

    time.sleep(sleeptime + 1)

    j.reload()
    assert j.resultCode == 200
    app.do_poll()

    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 10)


def test_retire(server_app, make_sleep_job, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerId = make_worker_id(
        "zerog", app.thisHost, app.name, app.pid
    )
    ctrlchannel = make_channel(workerId)

    sleeptime = 5
    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 30)

    msg = make_msg("retire")
    ctrlchannel.send_msg(msg)
    app.do_poll()
    assert app.state == "drainingRunning"

    clear_queue(updateschannel.queue)
    msg = make_msg("undrain")
    ctrlchannel.send_msg(msg)
    app.do_poll()
    assert app.state == "drainingRunning"

    clear_queue(updateschannel.queue)
    msg = make_msg("requestInfo")
    ctrlchannel.send_msg(msg)

    app.do_poll()
    infomsg = updateschannel.get_msg()
    assert infomsg is not None
    assert infomsg.msgtype == "info"
    assert infomsg.workerId == workerId
    assert infomsg.state == "drainingRunning"
    assert infomsg.retiring is True

    time.sleep(sleeptime + 1)

    j.reload()
    assert j.resultCode == 200
    app.do_poll()

    j = make_sleep_job(sleeptime)
    assert wait_until_running(j, 10) is False


def test_worker_manager(server_app, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerManager = WorkerManager("beanstalkd", 11300)
    workerManager.update_workers()
    app.do_poll()
    workerManager.poll_updates_channel()
    
    workers = workerManager.workers
    assert len(workers) == 1

    worker = list(workers.values())[0]
    assert "alive" in worker
    assert "state" in worker
    assert "retiring" in worker
    assert "runningJobUuid" in worker
    assert "mem" in worker

    assert worker['alive'] is True
    assert worker['state'] == "activeIdle"
    assert worker['retiring'] is False
    assert not worker['runningJobUuid']


def test_worker_manager_drain_host(server_app, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerManager = WorkerManager("beanstalkd", 11300)
    workerManager.update_workers()
    app.do_poll()
    workerManager.poll_updates_channel()

    workers = workerManager.workers_by_host()
    assert len(workers) == 1

    host = list(workers.keys())[0]
    workerManager.drain_host(host)
    app.do_poll()

    workerManager.update_workers()
    app.do_poll()
    workerManager.poll_updates_channel()

    workers = workerManager.workers
    assert len(workers) == 1

    worker = list(workers.values())[0]
    assert "state" in worker
    assert worker['state'] == "drainingIdle"


def test_worker_manager_retire_host(server_app, make_channel, clear_queue):
    updateschannel = make_channel("updates")
    clear_queue(updateschannel.queue)
    app = server_app([SleepJob])

    workerManager = WorkerManager("beanstalkd", 11300)
    workerManager.update_workers()
    app.do_poll()
    workerManager.poll_updates_channel()

    workers = workerManager.workers_by_host()
    assert len(workers) == 1

    host = list(workers.keys())[0]
    workerManager.drain_host(host, retire=True)
    app.do_poll()

    workerManager.update_workers()
    app.do_poll()
    workerManager.poll_updates_channel()

    workers = workerManager.workers
    assert len(workers) == 1

    worker = list(workers.values())[0]
    assert "state" in worker
    assert worker['state'] == "drainingIdle"
    assert worker['retiring'] is True
