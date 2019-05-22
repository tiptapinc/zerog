#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""

#
# NOTE: globals here not so good (in case multiple jobs run at once)
#

import datetime

from geyser.geyser_queue import queue_globals

import logging
log = logging.getLogger(__name__)

MIN_UPDATE_INTERVAL = datetime.timedelta(seconds=2)

CURRENT_JOB = None

WATCHDOG = None

RANGE_START = 0.0
RANGE_END = 1.0
RANGE_INTERVALS = 1
CURRENT_INTERVAL = 0

TRACK_COMPLETENESS = False


def set_watchdog(watchdog):
    global WATCHDOG

    WATCHDOG = watchdog


def keep_alive():
    if WATCHDOG:
        WATCHDOG()


def next_update_time():
    # only call if CURRENT_JOB exists!
    return CURRENT_JOB.updatedAt.replace(tzinfo=None) + MIN_UPDATE_INTERVAL


def update_ok(enforceMinInterval=False):
    ok = bool(CURRENT_JOB)

    if enforceMinInterval:
        ok = ok and datetime.datetime.utcnow() > next_update_time()

    return ok


def set_job(job, worker):
    global CURRENT_JOB
    global TRACK_COMPLETENESS

    CURRENT_JOB = job
    TRACK_COMPLETENESS = False


def unset_job():
    global CURRENT_JOB

    CURRENT_JOB = None


def set_completeness(completeness, enforceMinInterval=False):
    global CURRENT_JOB

    keep_alive()
    if update_ok(enforceMinInterval):
        CURRENT_JOB.update_attrs(completeness=completeness)


def track_completeness(start, end, intervals):
    global RANGE_START
    global RANGE_END
    global RANGE_INTERVALS
    global CURRENT_INTERVAL
    global TRACK_COMPLETENESS

    RANGE_START = start
    RANGE_END = end
    RANGE_INTERVALS = intervals
    CURRENT_INTERVAL = 0
    TRACK_COMPLETENESS = True


def increment_completeness():
    global RANGE_START
    global RANGE_END
    global RANGE_INTERVALS
    global CURRENT_INTERVAL
    global TRACK_COMPLETENESS

    keep_alive()
    if TRACK_COMPLETENESS:
        length = RANGE_END - RANGE_START
        CURRENT_INTERVAL += 1

        completeness = (
            RANGE_START +
            length * CURRENT_INTERVAL / RANGE_INTERVALS
        )
        set_completeness(min(completeness, RANGE_END), enforceMinInterval=True)


def info(msg):
    global CURRENT_JOB

    log.info(msg)
    keep_alive()

    if CURRENT_JOB:
        CURRENT_JOB.record_event(msg)


def warning(msg):
    global CURRENT_JOB

    log.warning(msg)
    keep_alive()

    if CURRENT_JOB:
        CURRENT_JOB.record_warning(msg)


def error_log_only(msg):
    global CURRENT_JOB

    log.error(msg)
    keep_alive()

    if CURRENT_JOB:
        raise queue_globals.WFErrorContinue


def error_continue(errorCode, msg):
    global CURRENT_JOB

    log.error(msg)
    keep_alive()

    if CURRENT_JOB:
        CURRENT_JOB.record_error(errorCode, msg, "")
        raise queue_globals.WFErrorContinue


def error_finish(errorCode, msg):
    global CURRENT_JOB

    log.error(msg)

    if CURRENT_JOB:
        CURRENT_JOB.record_error(errorCode, msg, "")
        CURRENT_JOB.record_result(errorCode, msg)
        raise queue_globals.WFErrorFinish
