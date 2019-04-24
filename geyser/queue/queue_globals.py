#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2017 MotiveMetrics. All rights reserved.

"""

# result codes
INTERNAL_ERROR = 500
ADWORDS_API_ERROR = 550
NO_RESULT = -1


class WFErrorContinue(Exception):
    pass


class WFErrorFinish(Exception):
    pass
