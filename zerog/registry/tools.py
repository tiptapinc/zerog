#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2020 MotiveMetrics. All rights reserved.

"""

import functools
import importlib
import operator
import pkgutil


def find_subclasses(cls):
    """
    Find all the currently imported subclasses of a class.

    Args:
        cls: class what you want to find the subclasses of
    """
    subs = cls.__subclasses__()
    if not subs:
        return []

    known = functools.reduce(
        operator.add,
        [find_subclasses(sub) for sub in subs],
        []
    )
    known += subs
    return list(set(known))


def import_submodules(package, recursive=True):
    """
    Import all submodules of a module, recursively, including subpackages

    Args:
        package: name of package or the actual module

    """
    if isinstance(package, str):
        package = importlib.import_module(package)

    results = {}

    for _, name, is_pkg in pkgutil.walk_packages(package.__path__):
        fullName = package.__name__ + '.' + name

        results[fullName] = importlib.import_module(fullName)

        if recursive and is_pkg:
            results.update(import_submodules(fullName))

    return results
