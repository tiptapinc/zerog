#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2021 MotiveMetrics. All rights reserved.

"""
import datetime
import json
from marshmallow import Schema, fields
from marshmallow.validate import OneOf

VALID_STATES = [
    "starting",
    "polling",
    "runningJob"
    "draining",
    "orphaned",
    "stopped"
]


##################################################################
# base class for messages. shouldn't ever be sent as-is
#
class BaseSchema(Schema):
    msgtype = fields.String()
    workerId = fields.String(required=True)
    timestamp = fields.DateTime(format="iso")


class BaseMsg(object):
    SCHEMA = BaseSchema
    MSG_TYPE = "base"

    def __init__(self, **kwargs):
        self.msgtype = kwargs.get('msgtype', self.MSG_TYPE)
        self.workerId = kwargs['workerId']
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
    uuid = fields.String(required=True)
    jobType = fields.String(required=True)
    action = fields.String(
        validate=OneOf(["start", "end"]),
        required=True
    )


class JobMsg(BaseMsg):
    SCHEMA = JobMsgSchema
    MSG_TYPE = "job"

    def __init__(self, **kwargs):
        super(JobMsg, self).__init__(**kwargs)
        self.uuid = kwargs['uuid']
        self.jobType = kwargs['jobType']
        self.action = kwargs['action']


class InfoMsgSchema(BaseSchema):
    state = fields.String(
        validate=OneOf(VALID_STATES),
        required=True
    )


class InfoMsg(BaseMsg):
    SCHEMA = InfoMsgSchema
    MSG_TYPE = "info"

    def __init__(self, **kwargs):
        super(InfoMsg, self).__init__(**kwargs)
        self.state = kwargs['state']


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


class StopPollingMsgSchema(BaseSchema):
    pass


class StopPollingMsg(BaseMsg):
    """
    tells a worker to stop polling for new jobs and enter the
    "draining" state
    """
    SCHEMA = StopPollingMsgSchema
    MSG_TYPE = "stopPolling"


class StartPollingMsgSchema(BaseSchema):
    pass


class StartPollingMsg(BaseMsg):
    """
    tells a worker to start polling for new jobs
    """
    SCHEMA = StartPollingMsgSchema
    MSG_TYPE = "startPolling"


##################################################################
# functions to create a message using keyword arguments or
# to recreate a message from its serialized self

def make_msg(msgtype, workerId, **kwargs):
    """
    creates a message from keyword arguments
    """
    msgClass = MSG_TYPE_TO_CLASS_MAP[msgtype]
    schema = msgClass.SCHEMA

    kwargs['msgtype'] = msgtype
    kwargs['workerId'] = workerId
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
    StopPollingMsg,
    StartPollingMsg
]

MSG_TYPE_TO_CLASS_MAP = {
    msgClass.MSG_TYPE: msgClass
    for msgClass in MSG_CLASSES
}
