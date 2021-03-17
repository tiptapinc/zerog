import zerog
from .channels import MgmtChannel
from .messages import make_msg
from .utils import parse_worker_id


class WorkerManager(object):
    def __init__(self, queueHost, queuePort):
        self.queueHost = queueHost
        self.queuePort = queuePort

        queue = self.get_queue(zerog.UPDATES_CHANNEL_NAME)
        self.updatesChannel = MgmtChannel(queue)

        self.ctrlChannels = {}
        self.jobRuns = {}
        self.workers = {}

    def get_queue(self, queueName):
        queue = zerog.BeanstalkdQueue(
            self.queueHost, self.queuePort, queueName
        )
        return queue

    def get_ctrl_channel(self, workerId):
        if workerId not in self.ctrlChannels:
            queue = self.get_queue(workerId)
            self.ctrlChannels[workerId] = MgmtChannel(queue)

        return self.ctrlChannels[workerId]

    def workers_by_host(self):
        workersByHost = {}
        for workerId, workerData in self.workers.items():
            parsed = parse_worker_id(workerId)

            host = parsed['host']
            if host not in workersByHost:
                workersByHost[host] = []

            workersByHost[host].append(
                dict(
                    workerId=workerId,
                    state=workerData['state'],
                    runningJobUuid=workerData['runningJobUuid']
                )
            )

        return workersByHost

    def drain_host(self, host):
        workerIds = [
            w['workerId']
            for w in self.workers_by_host().get(host, [])
        ]
        self.drain_workers(workerIds)

    def host_is_drained(self, host):
        workers = self.workers_by_host().get(host, [])
        if len(workers) == 0:
            return False

        drained = [
            (w['state'] == "draining" and not w['runningJobUuid'])
            for w in workers
        ]
        return drained

    def job_count_by_host(self):
        jobCounts = {
            host: len([w for w in workers if w['runningJobUuid']])
            for host, workers in self.workers_by_host().items()
        }
        return jobCounts

    def states_by_host(self):
        states = {
            host: [w['state'] for w in workers]
            for host, workers in self.workers_by_host().items()
        }
        return states

    def known_workers(self):
        """
        returns a dictionary of {workerId: workerData}, for all workers
        that are listening on a control channel queue.
        """
        # make sure this WorkerManager isn't keeping any ctrl channels open
        for channel in self.ctrlChannels.values():
            channel.detach()

        # note that queueName == workerId
        channelNames = self.updatesChannel.list_all_queues()
        workerData = {}
        for wid in channelNames:
            parsed = parse_worker_id(wid)
            if parsed:
                if self.updatesChannel.get_named_queue_watchers(wid) == 0:
                    # the queue exists but has no watchers, which means the
                    # associated worker is terminated but there are messages
                    # in the queue which are keeping it alive.
                    #
                    # so the solution is to empty out the queue and then
                    # delete its associated ctrlChannel.
                    #
                    # this is a mess and really just points out that zerog
                    # management channels and probably the zerog Queue classes
                    # need to be rethought.
                    #
                    # le sigh
                    channel = self.get_ctrl_channel(wid)
                    channel.empty()
                    del self.ctrlChannels[wid]
                else:
                    workerData[wid] = parsed

        # workerData = {
        #     wid: parsed for wid, parsed in
        #     {wid: parse_worker_id(wid) for wid in channelNames}.items()
        #     if parsed
        # }
        return workerData

    def send_ctrl_msg(self, workerId, msg):
        # workers listen to a control channel where queueName == workerId
        channel = self.get_ctrl_channel(workerId)
        channel.attach()
        channel.send_msg(msg)

    def drain_workers(self, workerIds):
        msg = make_msg("drain")
        for workerId in workerIds:
            self.send_ctrl_msg(workerId, msg)

    def request_worker_statuses(self, workerIds):
        # if there is an outstanding worker status request sitting in
        # this queue, that could be a sign that the worker is dead, and
        # just piling up worker status requests is only going to keep
        # the channel open
        #
        # can we count listeners to figure it out?
        msg = make_msg("requestInfo")
        for workerId in workerIds:
            self.send_ctrl_msg(workerId, msg)

    def update_workers(self):
        workerIds = self.known_workers()

        # clear out workers that are no longer listening on a control
        # channel
        missing = set(self.workers.keys()) - set(workerIds.keys())
        for workerId in missing:
            del(self.workers[workerId])

        self.request_worker_statuses(workerIds)

    def poll_updates_channel(self):
        while True:
            msg = self.updatesChannel.get_msg()
            if not msg:
                break

            if msg.msgtype == "job":
                self.handle_job_msg(msg)

            elif msg.msgtype == "info":
                self.handle_info_msg(msg)

    def handle_job_msg(self, msg):
        timestamp = msg.timestamp
        uuid = msg.uuid
        action = msg.action
        workerId = msg.workerId

        jobRun = self.jobRuns.get(uuid, {})
        jobRun[timestamp] = dict(workerId=workerId, action=action)
        self.jobRuns[uuid] = jobRun

    def handle_info_msg(self, msg):
        workerId = msg.workerId
        workerData = dict(
            alive=True,
            state=msg.state,
            runningJobUuid=msg.uuid
        )
        self.workers[workerId] = workerData
