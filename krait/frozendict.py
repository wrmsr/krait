# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections
import itertools
import sys


class frozendict(collections.Mapping):
    __slots__ = ('_dct', '_hash')

    def __new__(cls, *args, **kwargs):
        if len(args) == 1:
            [arg] = args
            if isinstance(arg, cls):
                return arg
        return super(frozendict, cls).__new__(cls)

    def __init__(self, *args, **kwargs):
        self._hash = None
        if len(args) > 1:
            raise TypeError()
        dct = {}
        if args:
            src, = args
            if isinstance(src, collections.Mapping):
                for k in src:
                    dct[k] = src[k]
            else:
                for k, v in src:
                    dct[k] = v
        for k, v in kwargs.items():
            dct[k] = v
        self._dct = dct

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self._dct)

    def __getitem__(self, key):
        return self._dct[key]

    def __len__(self):
        return len(self._dct)

    def __iter__(self):
        return iter(self._dct)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(frozenset(self._dct.items()))
        return self._hash

    def __eq__(self, other):
        return type(self) is type(other) and self._dct == other._dct

    def __ne__(self, other):
        return not (self == other)

    def __getstate__(self):
        return tuple(self.items())

    def __setstate__(self, t):
        self.__init__(t)

    def drop(self, *keys):
        keys = frozenset(keys)
        return type(self)((k, self[k]) for k in self if k not in keys)

    if sys.version_info[0] < 3:
        def set(self, *args, **kwargs):
            new = type(self)(*args, **kwargs)
            return type(self)(itertools.chain(self.iteritems(), new.iteritems()))

    else:
        def set(self, *args, **kwargs):
            new = type(self)(*args, **kwargs)
            return type(self)(itertools.chain(self.items(), new.items()))
