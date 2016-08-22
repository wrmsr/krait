# -*- coding: utf-8 -*-
from __future__ import absolute_import

from krait import common


def test_abstract_dict():
    @common.abstract
    class A(object):
        pass

    class B(A):
        pass
    B().__dict__
