# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections


def state(condition, message='State requirement not met', exception=RuntimeError):
    if not condition:
        raise exception(message)


def arg(condition, message='Argument requirement not met', exception=ValueError):
    if not condition:
        raise exception(message)


def is_(obj, expected):
    return state(obj is expected)


def is_instance(obj, cls):
    if not isinstance(obj, cls):
        raise TypeError(obj, cls)
    return obj


def is_subclass(obj, cls):
    if not issubclass(obj, cls):
        raise TypeError(obj, cls)
    return obj


def not_none(obj):
    if obj is None:
        raise TypeError(obj)
    return obj


def none(*objs):
    for obj in objs:
        if obj is not None:
            raise TypeError(obj)


def replacing(existing, new, expected=None):
    if existing != expected:
        raise TypeError(new)
    return new


def setting_new_item(dct, key, value):
    if key in dct:
        raise KeyError(key)
    dct[key] = value
    return value


def is_callable(obj):
    if not callable(obj):
        raise TypeError(obj)
    return obj


def is_iterable(obj):
    if not isinstance(obj, collections.Iterable):
        raise TypeError(obj)
    return obj


def empty(obj):
    it = iter(obj)
    try:
        extra = next(it)
    except StopIteration:
        return obj
    else:
        raise ValueError('Expected one', extra)


def one(obj):
    it = iter(obj)
    result = next(it)
    empty(it)
    return result


def one_of(obj, cls):
    return is_instance(one(obj), cls)


def all_of(obj, cls):
    return [is_instance(item, cls) for item in obj]
