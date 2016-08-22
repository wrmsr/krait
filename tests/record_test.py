# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

try:
    import cPickle as pickle
except ImportError:
    import pickle

from krait import six
from krait.record import RecordBuilder
from krait.record import record


@record()
class SomeRecord(object):
    some_string = record.field(six.string_types)
    some_int = record.field(int)

    def double_some_int(self):
        return self.some_int * 2


def test_abstract():
    t0 = RecordBuilder().add_field('x').build('t0', abstract=True)
    try:
        t0(1)
    except TypeError:
        pass
    else:
        assert False
    t1 = RecordBuilder().add_base(t0).build('t1')
    assert 1 == t1(1).x


def test_pickling():
    l = SomeRecord('hi', 2)
    r = pickle.loads(pickle.dumps(l))
    assert l == r
    assert tuple(r) == ('hi', 2)


def test_default():
    @record()
    class Thing(object):
        l = record.field(int)
        r = record.field((int, None), default=None)
    assert Thing(10).r is None
    assert 15 == Thing(10, 15).r


def test_populators():
    @record()
    class Thing(object):
        x = record.field(int)
        two_x = record.field(int, new=lambda x: x * 2)
    assert 20 == Thing(10).two_x
    assert 15 == Thing(10, 15).two_x


def test_dict():
    t = SomeRecord('what', 42)
    dct = {'some_string': 'what', 'some_int': 42}
    assert dct == t.__dict__


def test_inheritence():
    @record()
    class SomeRecord2(SomeRecord):
        some_float = record.field(float)
    t = SomeRecord2('uh', 11, 3.5)
    assert 3.5 == t.some_float
    assert 22 == t.double_some_int()


def test_validate():
    @record()
    class SomeRecord2(SomeRecord):
        record.validate(lambda some_int: some_int < 10, ValueError)
    assert 9 == SomeRecord2('what', 9).some_int
    try:
        SomeRecord2('what', 10)
    except ValueError:
        pass
    else:
        assert False


def test_replace():
    assert ('a', 3) == tuple(SomeRecord('a', 2)._replace(some_int=3))


def test_impossible_provision():
    rb = RecordBuilder().add_field('x', new=lambda x: x + 1)
    try:
        rb.build('t')
    except ValueError:
        pass
    else:
        assert False
    rb = RecordBuilder().add_field('x', new=lambda y: y + 1).add_field('y', new=lambda x: x + 1)
    try:
        rb.build('t')
    except ValueError:
        pass
    else:
        assert False


def test_eq():
    assert SomeRecord('a', 1) == SomeRecord('a', 1)
    assert not (SomeRecord('a', 1) != SomeRecord('a', 1))
    assert not (SomeRecord('a', 1) == SomeRecord('a', 2))
    assert SomeRecord('a', 1) != SomeRecord('a', 2)

    @record()
    class SomeRecord2(SomeRecord):
        pass
    assert not (SomeRecord('a', 1) == SomeRecord2('a', 1))
    assert SomeRecord('a', 1) != SomeRecord2('a', 1)


def test_hash():
    hash(SomeRecord('a', 1))


def test_cls_ref():
    @record()
    class What(object):
        record.validate(lambda _cls: _cls is What)
    What()


def test_descriptive_type_checks():
    try:
        SomeRecord('hi', 'there')
    except TypeError as t:
        assert 'some_int' in t.args[0]
    else:
        assert False


def test_callable():
    @record()
    class Thing(object):
        fn = record.field(callable)
    Thing(int)
    try:
        Thing(1)
    except TypeError:
        pass
    else:
        assert False


def test_het_type():
    @record()
    class Thing(object):
        t = record.field(record.het_type(tuple, int, float, str))
    Thing((1, 2.0, '3'))
    try:
        Thing((1, 2.0, 3))
    except TypeError:
        pass
    else:
        assert False


def test_all_type():
    class A(object):
        pass

    class B(object):
        pass

    class C(A, B):
        pass

    @record()
    class Thing(object):
        ab = record.field(record.all_type(A, B))
    Thing(C())
    try:
        Thing(A)
    except TypeError:
        pass
    else:
        assert False


class InnerThing(object):
    @record(pickle_path='InnerThing')
    class InnerInnerThing(object):
        value = record.field(six.string_types)


def test_pickle_path():
    thing = InnerThing.InnerInnerThing('hi there')
    assert thing == pickle.loads(pickle.dumps(thing))


def test_to_dict():
    @record(keyable=True)
    class Point(object):
        x = record.field(int)
        y = record.field(int)

    @record(keyable=True)
    class Rect(object):
        top_left = record.field(Point)
        bottom_right = record.field(Point)
        record.validate(
            lambda top_left, bottom_right: top_left.x <= bottom_right.x and top_left.y <= bottom_right.y)
    rect = Rect(Point(1, 2), Point(3, 4))
    rect_dct = rect._to_dict()
    assert {'top_left': {'x': 1, 'y': 2}, 'bottom_right': {'x': 3, 'y': 4}} == rect_dct
    assert rect == Rect._from_dict(rect_dct)

    @record()
    class PointList(object):
        points = record.field([Point])
    point_list = PointList([Point(1, 2), Point(3, 4)])
    point_list_dct = point_list._to_dict()
    assert point_list_dct == {'points': [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}]}
    assert point_list == PointList._from_dict(point_list_dct)

    @record()
    class PointsByRect(object):
        dct = record.field({Rect: Point})
    points_by_rect = PointsByRect({rect: Point(5, 6)})
    points_by_rect_dct = points_by_rect._to_dict()
    assert points_by_rect_dct == {'dct': {((1, 2), (3, 4)): {'x': 5, 'y': 6}}}
    assert points_by_rect == PointsByRect._from_dict(points_by_rect_dct)

    @record()
    class Config(object):
        dct = record.field({})
        some_set = record.field(frozenset)
        some_point_dct = record.field({six.string_types: Point})
        thing = record.field()
    cfg = Config(
        dct={'hi_there': 10},
        some_set=frozenset([1, 2, 3]),
        some_point_dct={'first': Point(1, 2), 'second': Point(3, 4)},
        thing=42)
    cfg_dct = json.loads(json.dumps(cfg._to_dict()))
    assert cfg == Config._from_dict(cfg_dct)
