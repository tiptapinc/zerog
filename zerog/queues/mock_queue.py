import json
import queue


class MockQueueJob(object):
    def __init__(self, queue, jid, body):
        self.queue = queue    # this is a queue.Queue object
        self.jid = jid
        self.body = json.dumps(body)
        self.reserves = 0
        self.timeouts = 0

    def delete(self):
        # assume job is reserved, so job has already been removed
        # from the queue
        return

    def release(self, **kwargs):
        # assume job is reserved, so releasing it back to the queue
        # means it needs to be re-enqueued
        self.queue.put(json.loads(self.body))

    def stats(self):
        return dict(reserves=self.reserves, timeouts=self.timeouts)


class MockQueue(object):
    def __init__(self):
        self.queue = queue.Queue()
        self.currentjid = 0

    def put(self, data, **kwargs):
        self.currentjid += 1
        job = MockQueueJob(self.queue, self.currentjid, data)
        self.queue.put(job)
        return True

    def reserve(self, **kwargs):
        """
        blocking not supported for mock queue
        """
        try:
            job = self.queue.get(False)
            job.reserves += 1

        except queue.Empty:
            job = None

        return job
