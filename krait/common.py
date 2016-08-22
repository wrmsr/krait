# -*- coding: utf-8 -*-
from __future__ import absolute_import

import abc
import contextlib
import datetime
import os
import random
import re
import sys
import tempfile

from .frozendict import frozendict

try:
    from collections import OrderedDict
except ImportError:
    try:
        from ordereddict import OrderedDict
    except ImportError:
        from .ordereddict import OrderedDict

try:
    import simplejson as json
except ImportError:
    import json

try:
    import pygments  # noqa
    import pygments.lexers  # noqa
    import pygments.formatters  # noqa
except ImportError:
    pygments = None


__all__ = [
    'json',
    'OrderedDict',
]


def public(target, *names, **kwargs):
    dct = kwargs.pop('dct', sys._getframe(1).f_locals)
    if kwargs:
        raise TypeError(kwargs)
    __all__ = dct.setdefault('__all__', [])
    subtargets = target if isinstance(target, tuple) else (target,)
    for subtarget in subtargets:
        subnames = names or (subtarget.__name__,)
        for subname in subnames:
            if subname in dct or subname in __all__:
                raise NameError(subname)
            dct[subname] = target
            __all__.append(subname)
    return target

__all__.append('public')


@public
def build_repr(obj, *attrs):
    return '%s(%s)' % (
        type(obj).__name__,
        ', '.join('%s=%r' % (attr, getattr(obj, attr)) for attr in attrs))


@public
def build_attr_repr(obj):
    return build_repr(obj, *obj.__repr_attrs__)


@public
def build_attr_repr_tuple(obj):
    return (type(obj),) + tuple(getattr(obj, attr) for attr in obj.__repr_attrs__)


@public
def buffer_seq(seq):
    if isinstance(seq, (list, tuple)):
        return seq
    return list(seq)


@public
def json_loads(s):
    return json.loads(s)


@public
def json_dumps(o):
    return json.dumps(o, indent=None, separators=(',', ':'))


@public
def is_json_encodable(obj):
    try:
        json.dumps(obj)
        return True
    except:
        return False


@public
def ojson_loads(s):
    import simplejson
    return simplejson.JSONDecoder(object_pairs_hook=OrderedDict).decode(s)


@public
class Nonlocal(object):
    pass


@public
def abstract(cls):
    dct = dict(cls.__dict__.items())
    if '__dict__' in dct:
        del dct['__dict__']
    return abc.ABCMeta(cls.__name__, cls.__bases__, dct)

abstract.method = abc.abstractmethod
abstract.property = abc.abstractproperty


@public
@contextlib.contextmanager
def fuse(*objs):
    if objs:
        with objs[0] as ret:
            with fuse(*objs[1:]) as rest:
                yield (ret,) + rest
    else:
        yield ()


@public
def to_secs(timedelta_value):
    return (86400 * timedelta_value.days +
            timedelta_value.seconds +
            0.000001 * timedelta_value.microseconds)


@public
def months_ago(current_month, months_ago):
    ago_year = current_month.year
    ago_month = current_month.month - months_ago
    while ago_month < 1:
        ago_year -= 1
        ago_month += 12
    while ago_month > 12:
        ago_year += 1
        ago_month -= 12

    return datetime.date(ago_year, ago_month, 1)


# match time deltas like '1d5m' or '5 hours, 2.5 seconds'
TIMEDELTA_DHMS_RE = re.compile(
    r'^\s*'
    r'(?P<negative>-)?'
    r'((?P<days>\d+(\.\d+)?)\s*(d|days?))?'
    r',?\s*((?P<hours>\d+(\.\d+)?)\s*(h|hours?))?'
    r',?\s*((?P<minutes>\d+(\.\d+)?)\s*(m|minutes?))?'
    r',?\s*((?P<seconds>\d+(\.\d+)?)\s*(s|secs?|seconds?))?'
    r'\s*$')

# match the format produced by timedelta.__str__, which attaches
# the negative sign to days only
TIMEDELTA_STR_RE = re.compile(
    r'^\s*'
    r'((?P<days>-?\d+)\s*days?,\s*)?'
    r'(?P<hours>\d?\d):(?P<minutes>\d\d)'
    r':(?P<seconds>\d\d+(\.\d+)?)'
    r'\s*$')


@public
def parse_date(a_date):
    if a_date.lower() in ['today', 'now']:
        return datetime.date.today()
    elif a_date.lower() == 'yesterday':
        return datetime.date.today() - datetime.timedelta(days=1)
    elif a_date.lower().endswith(' days ago'):
        num = int(a_date.split(' ', 1)[0])
        return datetime.date.today() - datetime.timedelta(days=num)
    elif a_date.lower().endswith(' months ago'):
        months = int(a_date.split(' ', 1)[0])
        return months_ago(datetime.date.today(), months)
    else:
        return datetime.date(*map(int, a_date.split('-', 3)))


@public
def parse_timedelta(value):
    match = TIMEDELTA_DHMS_RE.match(value)
    if not match:
        match = TIMEDELTA_STR_RE.match(value)
    if not match:
        raise ValueError()
    timedelta_kwargs = dict((k, float(v))
                            for k, v in match.groupdict().items()
                            if k != 'negative' and v is not None)
    if not timedelta_kwargs:
        raise ValueError()
    sign = -1 if match.groupdict().get('negative') else 1
    return sign * datetime.timedelta(**timedelta_kwargs)


@public
def import_module(dotted_path):
    if not dotted_path:
        raise ImportError(dotted_path)
    mod = __import__(dotted_path, globals(), locals(), [])
    for name in dotted_path.split('.')[1:]:
        try:
            mod = getattr(mod, name)
        except AttributeError:
            raise AttributeError("Module %r has no attribute %r" % (mod, name))
    return mod


@public
def import_module_class(dotted_path):
    module_name, _, class_name = dotted_path.rpartition('.')
    mod = import_module(module_name)
    try:
        attr = getattr(mod, class_name)
    except AttributeError:
        raise AttributeError("Module %r has no class %r" % (module_name, class_name))
    return attr


@public
def knuth_shuffle(x):
    for i in range(len(x) - 1, 0, -1):
        j = random.randrange(i + 1)
        x[i], x[j] = x[j], x[i]
    return x


@public
class ContextManager(object):

    def __enter__(self):
        return self

    def __exit__(self, et, e, tb):
        pass


@public
class NotInstantiable(object):

    def __new__(*args, **kwargs):
        raise TypeError()

    def __init__(*args, **kwargs):
        raise TypeError()


@public
class NotPicklable(object):

    def __getstate__(self):
        raise NotImplementedError('Cannot pickle')

    def __setstate__(self, t):
        raise NotImplementedError('Cannot pickle')


@public
def freeze(obj):
    if isinstance(obj, set):
        return frozenset(map(freeze, obj))
    elif isinstance(obj, list):
        return tuple(map(freeze, obj))
    elif isinstance(obj, dict):
        return frozendict(zip(map(freeze, obj.keys()), map(freeze, obj.values())))
    else:
        return obj


@public
class ItemAccessor(object):

    def __init__(self, fn):
        super(ItemAccessor, self).__init__()
        self.fn = fn

    def __getitem__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def get(self, key, default=None):
        try:
            return self.fn(key)
        except KeyError:
            return default


@public
def atomic_write_file(file_path, contents, mode='w'):
    base_path = os.path.dirname(file_path)
    fd, temp_file_path = tempfile.mkstemp(dir=base_path)
    try:
        os.write(fd, contents)
        os.fsync(fd)
    except Exception:
        raise
    finally:
        os.close(fd)
    os.rename(temp_file_path, file_path)
