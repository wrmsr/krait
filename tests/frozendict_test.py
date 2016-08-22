# -*- coding: utf-8 -*-
from __future__ import absolute_import

import pickle

from krait.frozendict import frozendict


def test():
    print(frozendict(x=1, y=2).set(x=10, z=20))
    print(pickle.loads(pickle.dumps(frozendict(x=1, y=2).set(x=10, z=20))))
