import zerog
from zerog.mgmt import MgmtChannel, make_msg, parse_worker_id


class WorkerManager(object):
    def __init__(self, **kwargs):
        queue = zerog.BeanstalkdQueue(
            "synapse",  # not cool
            8101,       # also not cool
            zerog.UPDATES_CHANNEL_NAME
        )
        self.updatesChannel = MgmtChannel(queue)
        self.ctrlChannels = {}
        self.jobRuns = {}
        self.workers = {}

    def get_ctrl_channel(self, workerId):
        if workerId not in self.ctrlChannels:
            queue = zerog.BeanstalkdQueue(
                "synapse",  # not cool
                8101,       # not cool
                workerId
            )
            self.ctrlChannels[workerId] = MgmtChannel(queue)

        return self.ctrlChannels[workerId]

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
        workerData = {
            wid: parsed for wid, parsed in
            {wid: parse_worker_id(wid) for wid in channelNames}.items()
            if parsed
        }
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
