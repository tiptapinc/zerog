#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2021 MotiveMetrics. All rights reserved.


current model: updates are sent in response to request on the control queue


response messages:
- info

state change triggered messages:
job started
job ended

message format:
messageType
timestamp
source (even though this is known from the queue)
data

"""
from zerog.mgmt.messages import make_msg, make_msg_from_json


class MgmtChannel(object):
    def __init__(self, queue):
        """
        Args:
            queue: zerog Queue object on which messages will be
                   produced/consumed
        """
        self.queue = queue

    def make_msg(self, msgtype, **kwargs):
        msg = make_msg(msgtype, **kwargs)
        return msg

    def send_msg(self, msg, **kwargs):
        """
        Args:
            msg: a zerog management message - subclass of messages.BaseMsg

            kwargs: keyword arguments passed through to queue's "put" method
        """
        self.queue.put(msg.dump(), **kwargs)

    def get_msg(self, **kwargs):
        """
        Args:
            kwargs: keyword arguments passed through to the queue's
                    "reserve" method

        Returns:
            next available message from the queue, or None if there are no
            available messages.

            Returned message will be a subclass of messages.BaseMsg
        """
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 0

        queueJob = self.queue.reserve(**kwargs)
        if queueJob:
            # could wrap this in a try:except to catch malformed messages
            # but they really shouldn't be happening so I think it's better
            # to let any exceptions trickle up
            msg = make_msg_from_json(queueJob.body)
            queueJob.delete()
            return msg

        return None

    def attach(self):
        """
        attaches this instance to both use the named queue for sends and
        to watch the named queue for gets

        when queues are created, they are automatically attached, so this
        method is only needed after a detach
        """
        self.queue.attach()

    def detach(self):
        """
        detaches this instance from watching and sending on the named
        queue, which frees the queue to close if no instances are
        attached.
        """
        self.queue.detach()

    def list_all_queues(self):
        """
        returns a list of all available named queues, not just the one
        named for this instance
        """
        return self.queue.list_all_queues()
