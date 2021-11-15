#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2021 MotiveMetrics. All rights reserved.

"""
import datetime
import json
from marshmallow import Schema, fields
from marshmallow.validate import OneOf


##################################################################
# base class for messages. shouldn't ever be sent as-is
#
class BaseSchema(Schema):
    msgtype = fields.String()
    timestamp = fields.DateTime(format="iso")


class BaseMsg(object):
    SCHEMA = BaseSchema
    MSG_TYPE = "base"

    def __init__(self, **kwargs):
        self.msgtype = kwargs.get('msgtype', self.MSG_TYPE)
        self.timestamp = kwargs.get('timestamp', datetime.datetime.utcnow())

    def dump(self):
        return self.SCHEMA().dump(self)

    def dumps(self, **kwargs):
        return self.SCHEMA().dumps(self, **kwargs)

    def __str__(self):
        return self.dumps(indent=4)


##################################################################
# update messages are used to send information from workers
# to a manager

class JobMsgSchema(BaseSchema):
    workerId = fields.String(required=True)
    uuid = fields.String(required=True)
    action = fields.String(
        validate=OneOf(["start", "end"]),
        required=True
    )


class JobMsg(BaseMsg):
    SCHEMA = JobMsgSchema
    MSG_TYPE = "job"

    def __init__(self, **kwargs):
        super(JobMsg, self).__init__(**kwargs)
        self.workerId = kwargs['workerId']
        self.uuid = kwargs['uuid']
        self.action = kwargs['action']


class InfoMsgSchema(BaseSchema):
    workerId = fields.String(required=True)
    state = fields.String(required=True)
    uuid = fields.String()
    mem = fields.Dict()
    retiring = fields.Boolean()


class InfoMsg(BaseMsg):
    SCHEMA = InfoMsgSchema
    MSG_TYPE = "info"

    def __init__(self, **kwargs):
        super(InfoMsg, self).__init__(**kwargs)
        self.workerId = kwargs['workerId']
        self.state = kwargs['state']
        self.uuid = kwargs.get('uuid', "")
        self.mem = kwargs.get('mem', {})
        self.retiring = kwargs.get('retiring', False)


##################################################################
# control messages are used by the manager to tell a worker to take
# some action

class RequestInfoMsgSchema(BaseSchema):
    pass


class RequestInfoMsg(BaseMsg):
    """
    tells a worker to send an info message on the updates channel
    """
    SCHEMA = RequestInfoMsgSchema
    MSG_TYPE = "requestInfo"


class KillJobMsgSchema(BaseSchema):
    uuid = fields.String(required=True)


class KillJobMsg(BaseMsg):
    """
    tells a worker to kill a running job with the specified uuid
    """
    SCHEMA = KillJobMsgSchema
    MSG_TYPE = "killJob"

    def __init__(self, **kwargs):
        super(KillJobMsg, self).__init__(**kwargs)
        self.uuid = kwargs['uuid']


class DrainMsgSchema(BaseSchema):
    pass


class DrainMsg(BaseMsg):
    """
    tells a worker to stop polling for new jobs and enter the
    "draining" state
    """
    SCHEMA = DrainMsgSchema
    MSG_TYPE = "drain"


class UnDrainMsgSchema(BaseSchema):
    pass


class UnDrainMsg(BaseMsg):
    """
    tells a worker to exit the draining state and start polling for
    new jobs again. If the worker is in the retiring state, this should
    do nothing
    """
    SCHEMA = UnDrainMsgSchema
    MSG_TYPE = "undrain"


class RetireMsgSchema(BaseSchema):
    pass


class RetireMsg(BaseMsg):
    """
    tells a worker to stop polling for new jobs and enter the
    draining and retiring states. The retiring state is meant
    to be irreversible.
    """
    SCHEMA = RetireMsgSchema
    MSG_TYPE = "retire"


##################################################################
# functions to create a message using keyword arguments or
# to recreate a message from its serialized self

def make_msg(msgtype, **kwargs):
    """
    creates a message from keyword arguments
    """
    msgClass = MSG_TYPE_TO_CLASS_MAP[msgtype]
    schema = msgClass.SCHEMA

    kwargs['msgtype'] = msgtype
    loaded = schema().load(kwargs)
    msg = msgClass(**loaded)

    return msg


def make_msg_from_json(jsonStr):
    """
    reconstitutes a message from its JSON serialized self
    """
    kwargs = json.loads(jsonStr)
    msg = make_msg(**kwargs)
    return msg


MSG_CLASSES = [
    JobMsg,
    InfoMsg,
    RequestInfoMsg,
    KillJobMsg,
    DrainMsg,
    UnDrainMsg,
    RetireMsg
]

MSG_TYPE_TO_CLASS_MAP = {
    msgClass.MSG_TYPE: msgClass
    for msgClass in MSG_CLASSES
}


##################################################################
# functions to send or retrieve messages on a specified queue
# connection/tube combination

def send_msg(msg, queue, tube, **kwargs):
    """
    send a message on a queue connection / tube by temporarily using
    the named tube.

    currently makes assumptions about the 'queue' object's internal
    workings (not good)

    Args:
        msg: a zerog management message - subclass of messages.BaseMsg

        queue: a zerog.BeanstalkdQueue object.

        tube: the name of a Beanstalkd tube (aka "queueName" elsewhere)

        kwargs: keyword arguments passed through to queue's "put" method
    """
    queue.do_bean("use", tube)
    queue.put(msg.dump(), **kwargs)
    queue.do_bean("use", "default")


def get_msg(queue, tube, **kwargs):
    """
    get a message if one is available from a queue connection / tube
    by temporarily watching the named tube.

    currently makes assumptions about the 'queue' object's internal
    workings (not good)

    Args:
        queue: a zerog.BeanstalkdQueue object.

        tube: the name of a Beanstalkd tube (aka "queueName" elsewhere)

        kwargs: keyword arguments passed through to the queue's
                "reserve" method

    Returns:
        next available message from the queue, or None if there are no
        available messages.

        Returned message will be a subclass of messages.BaseMsg
    """
    queue.do_bean("watch", tube)

    if 'timeout' not in kwargs:
        kwargs['timeout'] = 0

    msg = None
    queueJob = queue.reserve(**kwargs)
    if queueJob:
        # could wrap this in a try:except to catch malformed messages
        # but they really shouldn't be happening so I think it's better
        # to let any exceptions trickle up
        msg = make_msg_from_json(queueJob.body)
        queueJob.delete()

    queue.do_bean("ignore", tube)
    return msg
