# -*- coding: utf-8 -*-
"""A better namedtuple.

Compared to namedtuple supports:

* class-style definition
* args, kwargs with defaults, kwargs with default populators
* validation code
* [multiple] inheritance
* arbitrary additional class contents without requiring inheritance
* type-honoring equality checks
* not-so-stupid type checking
* abstract classes
* pickling support for nested classes

Further, in a limited capacity, it supports:

* going to/from a simple dict representation (suitable for JSON)
* installation and translation support for optparse

Aside from these provides the same functionality as namedtuple. And as with namedtuple don't reuse
field names or give it field names beginning with an underscore. Provides a fluent RecordBuilder, but
the preferred interface is simply 'record'. Example:

>>> import math
... from record import record
...
... @record()
... class Point(object):
...     x = record.field(float, default=0.0)
...     y = record.field(float, default=0.0)
...     dist = record.field(float, new=lambda x, y: math.sqrt(x**2 + y**2))
...     record.validate(lambda x, y: x >= 0 and y >= 0)
...
...     def draw(self):
...         print('.. uh.. %f %f' % (self.x, self.y))

Note that the arg to record.validate and the new kwarg in record.field are callables taking field
names as args. They will be passed the corresponding field values in __new__. Dependencies are
topologically sorted. An arg named _cls will be naturally filled with the class of the Record being
constructed. All population and validation happens before final construction of the tuple, so no
invalid instance of one should ever exist.

Constructor args are ordered with all non-default args first followed by all defaulted kwargs, each
including all base record class fields in reverse mro. The final order is visible in the docstring,
in the repr, and via the _fields class attribute.

The type specification expressions understood by build_type_specification are, recursively, as
follows:

* None -> NoneType
* callable -> callable (not really a type, but real-world useful enough to be special cased)
* tuple of any of these with len > 1 -> any one of its content specs, as with isinstance
* flat collection of any one of these -> instance of that collection with contents of contained spec
* mapping collection with one item -> instance of that collection with keys and values of contained
  specs
* type -> instance of that type
* TypeSpecification -> that TypeSpecification

TODO:
- @classmethod _doc ala optparse --help
- weakref TypeSpecificationBuilders (not a big deal cause 2.6.7 sucks at weakrefs [abc])
- pprint
- fix inheritance stupidity
- auto struct pack/unpack
- @record.dictspec()
- audit override
- get source of failed
- optional pyrsistent backing
- minimal revalidation/coercion when _replacing
- prob ditch optparse shit
- add def _validate for asserted_validations to manually check
- multiple input populators, priorities
"""
from __future__ import absolute_import

import abc
import collections
import functools
import inspect
import itertools
import operator
import sys


try:
    from collections import OrderedDict as RecordDict
except ImportError:
    try:
        from ordereddict import OrderedDict as RecordDict
    except ImportError:
        try:
            from .ordereddict import OrderedDict as RecordDict
        except ImportError:
            RecordDict = dict


try:
    assert False
    ASSERTIONS_ENABLED = False
except AssertionError:
    ASSERTIONS_ENABLED = True


if sys.version_info[0] > 2:
    try:
        exec_ = __builtins__['exec']
    except TypeError:
        exec_ = getattr(__builtins__, 'exec')
    reduce = functools.reduce
    string_type = str
else:
    def exec_(_code_, _globs_=None, _locs_=None):
        if _globs_ is None:
            frame = sys._getframe(1)
            _globs_ = frame.f_globals
            if _locs_ is None:
                _locs_ = frame.f_locals
            del frame
        elif _locs_ is None:
            _locs_ = _globs_
            exec("""exec _code_ in _globs_, _locs_""")
    string_type = basestring


class Record(tuple):
    __slots__ = ()


def marker(name, _repr=None):
    if _repr is None:
        _repr = name

    class Marker(object):
        __slots__ = ()

        def __repr__(self):
            return _repr

    return type(name, (Marker,), {})()

NOT_SET = marker('NOT_SET')


class NamePlaceholder(object):
    __slots__ = ()

    def __repr__(self):
        return '$%s@%x' % (type(self).__name__, id(self))


class TypeSpecification(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def checker(self):
        raise NotImplementedError()

    def to_dict_item(self, obj, for_key=False):
        raise TypeError('Unsupported', self, obj, for_key)

    def from_dict_item(self, obj):
        raise TypeError('Unsupported', self, obj)

    @property
    def optparse_kwargs(self):
        raise TypeError('Unsupported', self)

    class Builder(object):
        __metaclass__ = abc.ABCMeta

        @abc.abstractmethod
        def guard(self, obj):
            raise NotImplementedError()

        @abc.abstractmethod
        def __call__(self, obj):
            raise NotImplementedError()

    builders = []

    @classmethod
    def register_builder(cls, builder, first=False):
        if not isinstance(builder, cls.Builder):
            raise TypeError(type(builder), builder)
        if first:
            cls.builders.insert(0, builder)
        else:
            cls.builders.append(builder)

    @classmethod
    def builder(cls, guard, register=True, first=False):
        def inner(fn):
            dct = dict((k, getattr(fn, k)) for k in functools.WRAPPER_ASSIGNMENTS)
            dct.update({'guard': staticmethod(guard), '__call__': staticmethod(fn)})
            builder = type(fn.__name__, (cls.Builder,), dct)()
            if register:
                cls.register_builder(builder, first=first)
        return inner

    @classmethod
    def build(cls, obj):
        for builder in cls.builders:
            if builder.guard(obj):
                built = builder(obj)
                if not isinstance(built, TypeSpecification):
                    raise TypeError(type(built), built)
                return built
        raise TypeError(type(obj), obj)


class SimpleTypeSpecification(TypeSpecification):

    def __init__(self, _type):
        if not isinstance(_type, type):
            raise TypeError(_type)
        self.type = _type

    def __repr__(self):
        return 'Simple(%r)' % (self.type,)

    @property
    def checker(self):
        _type = self.type
        _isinstance = isinstance

        def fn(obj):
            return _isinstance(obj, _type)
        return fn

    def to_dict_item(self, obj, for_key=False):
        if issubclass(self.type, Record):
            return obj._to_dict(for_key=for_key)
        return obj

    def from_dict_item(self, obj):
        if issubclass(self.type, Record):
            return self.type._from_dict(obj)
        return obj

    @property
    def optparse_kwargs(self):
        if self.type is bool:
            return {'action': 'store_true'}
        else:
            return {'type': self.type}


class CallableTypeSpecification(TypeSpecification):

    def __repr__(self):
        return 'Callable'

    @property
    def checker(self):
        return callable

    def to_dict_item(self, obj, for_key=False):
        return obj

    def from_dict_item(self, obj):
        return obj


class CompositeTypeSpecification(TypeSpecification):

    def __init__(self, *sub_specs):
        for sub_spec in sub_specs:
            if not isinstance(sub_spec, TypeSpecification):
                raise TypeError(sub_spec)
        self.sub_specs = sub_specs

    def __repr__(self):
        return '%s%r' % (type(self).__name__.partition('TypeSpecification')[0], self.sub_specs,)


class AnyTypeSpecification(CompositeTypeSpecification):

    @property
    def checker(self):
        sub_checkers = tuple(sub_spec.checker for sub_spec in self.sub_specs)
        _any = any

        def fn(obj):
            return _any(sub_checker(obj) for sub_checker in sub_checkers)
        return fn

    def _besides(self, filter_fn):
        if len(self.sub_specs) == 2:
            l = list(filter(lambda s: not filter_fn(s), self.sub_specs))
            if len(l) == 1:
                return l[0]
        return None

    _besides_none = NOT_SET

    @property
    def besides_none(self):
        if self._besides_none is NOT_SET:
            self._besides_none = self._besides(
                lambda s: isinstance(s, SimpleTypeSpecification) and s.type is type(None))  # noqa
        return self._besides_none

    _besides_callable = NOT_SET

    @property
    def besides_callable(self):
        if self._besides_callable is NOT_SET:
            self._besides_callable = self._besides(lambda s: isinstance(s, CallableTypeSpecification))
        return self._besides_callable

    def to_dict_item(self, obj, for_key=False):
        if self.besides_none is not None:
            return self.besides_none.to_dict_item(obj, for_key=for_key) if obj is not None else None
        return super(AnyTypeSpecification, self).to_dict_item(obj, for_key=for_key)

    def from_dict_item(self, obj):
        if self.besides_none is not None:
            return self.besides_none.from_dict_item(obj) if obj is not None else None
        if self.besides_callable is not None:
            return self.besides_callable.from_dict_item(obj) if not callable(obj) else obj
        return super(AnyTypeSpecification, self).from_dict_item(obj)

    @property
    def optparse_kwargs(self):
        if self.besides_none is not None:
            return self.besides_none.optparse_kwargs
        return super(AnyTypeSpecification, self).optparse_kwargs


class AllTypeSpecification(CompositeTypeSpecification):

    @property
    def checker(self):
        sub_checkers = tuple(sub_spec.checker for sub_spec in self.sub_specs)
        _all = all

        def fn(obj):
            return _all(sub_checker(obj) for sub_checker in sub_checkers)
        return fn


class CollectionTypeSpecification(TypeSpecification):

    def __init__(self, col_spec, item_spec):
        if not isinstance(col_spec, TypeSpecification):
            raise TypeError(col_spec)
        if not isinstance(item_spec, TypeSpecification):
            raise TypeError(item_spec)
        self.col_spec = col_spec
        self.item_spec = item_spec

    def __repr__(self):
        return 'Collection(%r, %r)' % (self.col_spec, self.item_spec)

    @property
    def checker(self):
        col_checker = self.col_spec.checker
        item_checker = self.item_spec.checker
        _all = all

        def fn(obj):
            return col_checker(obj) and _all(item_checker(item) for item in obj)
        return fn

    def to_dict_item(self, obj, for_key=False):
        container = tuple if for_key else list
        return container(self.item_spec.to_dict_item(v, for_key=for_key) for v in obj)

    def from_dict_item(self, obj):
        if isinstance(self.col_spec, SimpleTypeSpecification):
            return self.col_spec.type(self.item_spec.from_dict_item(v) for v in obj)
        return super(CollectionTypeSpecification, self).from_dict_item(obj)

    @property
    def optparse_kwargs(self):
        if isinstance(self.item_spec, SimpleTypeSpecification) and self.item_spec.type is not bool:
            return {'type': self.item_spec.type, 'action': 'append'}
        return super(CollectionTypeSpecification, self).optparse_kwargs


class HeterogeneousCollectionTypeSpecification(TypeSpecification):

    def __init__(self, col_spec, *item_specs):
        if not isinstance(col_spec, TypeSpecification):
            raise TypeError(col_spec)
        if not all(isinstance(item_spec, TypeSpecification) for item_spec in item_specs):
            raise TypeError(item_specs)
        self.col_spec = col_spec
        self.item_specs = item_specs

    def __repr__(self):
        return 'HeterogeneousCollection(%r, %r)' % (self.col_spec, self.item_specs)

    @property
    def checker(self):
        col_checker = self.col_spec.checker
        item_checkers = tuple(item_spec.checker for item_spec in self.item_specs)
        _len = len
        _all = all
        _zip = zip

        def fn(obj):
            return col_checker(obj) and _len(obj) == _len(item_checkers) and \
                _all(item_checker(item) for item_checker, item in _zip(item_checkers, obj))
        return fn


class MappingTypeSpecification(TypeSpecification):

    def __init__(self, col_spec, key_spec, value_spec):
        if not isinstance(col_spec, TypeSpecification):
            raise TypeError(col_spec)
        if not isinstance(key_spec, TypeSpecification):
            raise TypeError(key_spec)
        if not isinstance(value_spec, TypeSpecification):
            raise TypeError(value_spec)
        self.col_spec = col_spec
        self.key_spec = key_spec
        self.value_spec = value_spec

    def __repr__(self):
        return 'Mapping(%r, %r, %r)' % (self.col_spec, self.key_spec, self.value_spec)

    @property
    def checker(self):
        col_checker = self.col_spec.checker
        key_checker = self.key_spec.checker
        value_checker = self.value_spec.checker
        _all = all

        def fn(obj):
            return col_checker(obj) and _all(key_checker(k) and value_checker(v) for k, v in obj.items())
        return fn

    def to_dict_item(self, obj, for_key=False):
        if not for_key:
            return dict(
                (self.key_spec.to_dict_item(k, for_key=True), self.value_spec.to_dict_item(v)) for k, v in obj.items())
        return super(MappingTypeSpecification, self).to_dict_item(obj, for_key=for_key)

    def from_dict_item(self, obj):
        if isinstance(self.col_spec, SimpleTypeSpecification) and isinstance(self.key_spec, SimpleTypeSpecification):
            return self.col_spec.type(
                (self.key_spec.from_dict_item(k), self.value_spec.from_dict_item(v)) for k, v in obj.items())
        return super(MappingTypeSpecification, self).from_dict_item(obj)


COLLECTION_TYPES = [list, tuple, set, frozenset]
MAPPING_COLLECTION_TYPES = [dict]


class TypeSpecificationBuilders(object):

    def __new__(*args, **kwargs):
        raise TypeError()

    @TypeSpecification.builder(lambda obj: obj is None)
    def none_builder(obj):
        return TypeSpecification.build(type(None))

    @TypeSpecification.builder(lambda obj: obj is callable)
    def callable_builder(obj):
        return CallableTypeSpecification()

    @TypeSpecification.builder(lambda obj: isinstance(obj, TypeSpecification))
    def type_specification_builder(obj):
        return obj

    @TypeSpecification.builder(lambda obj: isinstance(obj, tuple) and len(obj) > 1)
    def tuple_builder(obj):
        return AnyTypeSpecification(
            *[TypeSpecification.build(item) for item in obj])

    @TypeSpecification.builder(lambda obj: isinstance(obj, tuple(COLLECTION_TYPES)))
    def collection_builder(obj):
        if not obj:
            return CollectionTypeSpecification(
                SimpleTypeSpecification(type(obj)),
                TypeSpecification.build(object))
        else:
            [item] = obj
            return CollectionTypeSpecification(
                SimpleTypeSpecification(type(obj)),
                TypeSpecification.build(item))

    @TypeSpecification.builder(lambda obj: isinstance(obj, tuple(MAPPING_COLLECTION_TYPES)))
    def mapping_builder(obj):
        if not obj:
            return MappingTypeSpecification(
                SimpleTypeSpecification(type(obj)),
                TypeSpecification.build(object),
                TypeSpecification.build(object))
        else:
            [[key, value]] = obj.items()
            return MappingTypeSpecification(
                SimpleTypeSpecification(type(obj)),
                TypeSpecification.build(key),
                TypeSpecification.build(value))

    @TypeSpecification.builder(
        lambda obj: isinstance(obj, type) and (not issubclass(obj, Record)) and
        issubclass(obj, tuple(COLLECTION_TYPES)))
    def collection_type_builder(obj):
        return CollectionTypeSpecification(
            SimpleTypeSpecification(obj),
            TypeSpecification.build(object))

    @TypeSpecification.builder(
        lambda obj: isinstance(obj, type) and issubclass(obj, tuple(MAPPING_COLLECTION_TYPES)))
    def mapping_collection_type_builder(obj):
        return MappingTypeSpecification(
            SimpleTypeSpecification(obj),
            TypeSpecification.build(object),
            TypeSpecification.build(object))

    @TypeSpecification.builder(lambda obj: isinstance(obj, type))
    def simple_builder(obj):
        return SimpleTypeSpecification(obj)


def build_attr_repr(obj, attrs):
    return '%s@%x(%s)' % (
        type(obj).__name__, id(obj), ', '.join(
            '%s=%r' % (k, getattr(obj, k)) for k in attrs))


def toposort(data):
    for k, v in data.items():
        v.discard(k)
    extra_items_in_deps = reduce(set.union, data.values()) - set(data.keys())
    data.update(dict((item, set()) for item in extra_items_in_deps))
    while True:
        ordered = set(item for item, dep in data.items() if not dep)
        if not ordered:
            break
        yield ordered
        data = dict((item, (dep - ordered))
                    for item, dep in data.items() if item not in ordered)
    if data:
        raise ValueError("Cyclic dependencies exist among these items:\n%s" %
                         '\n'.join(repr(x) for x in data.items()))


class CallableSpecification(object):

    def __init__(self, fn, args=NOT_SET):
        if not callable(fn):
            raise TypeError(fn)
        if args is NOT_SET:
            argspec = inspect.getargspec(fn)
            if argspec.varargs or argspec.keywords or argspec.defaults:
                raise TypeError(argspec)
            args = tuple(argspec.args)
        if not isinstance(args, tuple) and all(isinstance(arg, str) for arg in args):
            raise TypeError(args)
        self.fn = fn
        self.args = args

    def __repr__(self):
        return build_attr_repr(self, ('fn', 'args'))

    @classmethod
    def of(cls, obj):
        if isinstance(obj, cls):
            return obj
        return cls(obj)


class FieldSpecification(object):

    def __init__(
        self,
        builder,
        name,
        type=NOT_SET,
        default=NOT_SET,
        new=NOT_SET,
        coerce=NOT_SET,
        coerce_=NOT_SET,
        doc=None,
        group=None,
        option_name=None,
        override=False
    ):
        self.builder = builder
        self._name = name
        self._resolved_name = None
        self.type = type
        self.default = default
        self.new = new
        self.coerce = coerce
        self.coerce_ = coerce_
        self.doc = doc
        self.group = group
        self.option_name = option_name
        self.override = override
        self.type_specification = builder.build_type_specification(self.type) if type is not NOT_SET else None

    def __repr__(self):
        return build_attr_repr(self, (
            'name',
            'type',
            'default',
            'new',
            'coerce',
            'coerce_',
            'doc',
            'group',
            'option_name',
            'override',
        ))

    @property
    def name(self):
        if self._resolved_name is None:
            try:
                self._resolved_name = self.builder.resolve_name(self._name)
            except NameError:
                return self._name
        return self._resolved_name


class PopulatorSpecification(object):

    def __init__(self, builder, field_name, populator):
        self.builder = builder
        self.field_name = field_name
        self.populator = CallableSpecification.of(populator)

    def __repr__(self):
        return build_attr_repr(self, ('field_name', 'populator'))


class ValidatorSpecification(object):

    def __init__(self, builder, validate, exception=ValueError):
        self.builder = builder
        self.validate = CallableSpecification.of(validate)
        self.exception = exception

    def __repr__(self):
        return build_attr_repr(self, ('validate', 'exception'))


class RecordBuilder(object):

    def __init__(self):
        self.bases = []
        self.names_by_placeholder = {}
        self.dict = {}
        self.fields = []
        self.validators = []
        self.populators_by_field_name = {}
        self.field_group_docs = {}

    def add_field(self, *args, **kwargs):
        field = FieldSpecification(self, *args, **kwargs)
        self.fields.append(field)
        return self

    def add_populator(self, *args, **kwargs):
        populator = PopulatorSpecification(self, *args, **kwargs)
        self.populators_by_field_name[populator.field_name] = populator
        return self

    def add_validator(self, *args, **kwargs):
        validator = ValidatorSpecification(self, *args, **kwargs)
        self.validators.append(validator)
        return self

    def add_base(self, base):
        self.bases.append(base)
        return self

    def set_field_group_doc(self, group, doc):
        self.field_group_docs[group] = doc
        return self

    def update_dict(self, dict_updates):
        self.dict.update(dict_updates)
        return self

    def get_type_validators(self, type_name):
        for field in self.fields:
            if field.type_specification is not None:
                type_spec = field.type_specification

                def raiser(name, type_spec, obj):
                    raise TypeError('%s.%s must be of type %r, not value %r of type %r' % (
                        type_name, name, type_spec, obj, type(obj)))
                yield ValidatorSpecification(
                    self,
                    CallableSpecification(type_spec.checker, (field.name,)),
                    CallableSpecification(functools.partial(raiser, field.name, type_spec), (field.name,)))

    def get_all_type_validators(self, type_name):
        for builder in itertools.chain(self.base_builders, [self]):
            for type_validator in builder.get_type_validators(type_name):
                yield type_validator

    def get_all_validators(self):
        for builder in itertools.chain(self.base_builders, [self]):
            for validator in builder.validators:
                yield validator

    def build_type_specification(self, type):
        return TypeSpecification.build(type)

    @property
    def base_builders(self):
        seen = set()
        for base in self.bases:
            for cls in base.__mro__:
                if issubclass(cls, Record) and cls is not Record:
                    try:
                        builder = cls.__dict__['_builder']
                    except KeyError:
                        pass
                    else:
                        if builder not in seen:
                            seen.add(builder)
                            yield builder

    def get_all_override_fields(self):
        dct = {}
        for builder in itertools.chain(reversed(list(self.base_builders)), [self]):
            for field in builder.fields:
                if field.override:
                    dct.setdefault(field.name, field)
        return dct

    def get_all_fields(self):
        overrides = self.get_all_override_fields()
        seen = set()
        for builder in itertools.chain(reversed(list(self.base_builders)), [self]):
            for field in builder.fields:
                if field.override:
                    continue
                if field.name in seen:
                    raise NameError('Duplicate field name: %r' % (field.name,))
                if field.name in overrides:
                    yield overrides.pop(field.name)
                else:
                    yield field
                seen.add(field.name)
        if overrides:
            raise NameError('Overridden field not preseent: %r' % (', '.join(overrides)))

    def get_all_populators_by_field_name(self):
        fields = self.get_all_fields()
        populators_by_field_name = dict(
            (f.name, PopulatorSpecification(self, f.name, f.new)) for f in fields if f.new is not NOT_SET)

        for builder in itertools.chain(reversed(list(self.base_builders)), [self]):
            for name, pop in builder.populators_by_field_name.items():
                if name not in populators_by_field_name:
                    populators_by_field_name[name] = pop
        return populators_by_field_name

    def are_validations_enabled(self, dct):
        if ASSERTIONS_ENABLED:
            return True
        if '_asserted_validations' in dct:
            return not dct['_asserted_validations']
        for base in self.base_builders:
            if '_asserted_validations' in base.dict:
                return not base.dict['_asserted_validations']
        return True

    def get_field_group_doc(self, group):
        for builder in itertools.chain([self], reversed(list(self.base_builders))):
            try:
                return builder.field_group_docs[group]
            except KeyError:
                continue
        return None

    def resolve_name_placeholders(self, names_by_placeholder):
        for k, v in names_by_placeholder.items():
            if not isinstance(k, NamePlaceholder):
                raise TypeError(k)
            if not isinstance(v, str):
                raise TypeError(v)
            if k in self.names_by_placeholder:
                raise NameError('Duplicate resolved name: %r' % (k,))
            self.names_by_placeholder[k] = v
        return self

    def resolve_name(self, name):
        if isinstance(name, str):
            return name
        elif not isinstance(name, NamePlaceholder):
            raise TypeError(name)
        resolved_names = frozenset(
            resolved_name for resolved_name in
            [self.names_by_placeholder.get(name, NOT_SET)] +
            [base_builder.names_by_placeholder.get(name, NOT_SET) for base_builder in self.base_builders]
            if resolved_name is not NOT_SET)
        if not resolved_names:
            raise NameError(name)
        elif len(resolved_names) != 1:
            raise NameError((name, resolved_names))
        [resolved_name] = resolved_names
        if not isinstance(resolved_name, str):
            raise TypeError(resolved_name)
        return resolved_name

    ABSTRACT_CHECK_CODE = """
    raise TypeError('{name} is abstract')
"""

    def build_abstract_check_code(self, name, abstract, dct):
        dct['_abstract'] = abstract
        if not abstract:
            return ''
        else:
            return self.ABSTRACT_CHECK_CODE.format(name=name)

    VALIDATOR_CODE = """
    if not {validator_name}({validator_args}):
        {validator_raise}
"""

    def build_validators_code(self, validators, ctor_namespace):
        validators_code = ''
        for idx, validator in enumerate(validators):
            vname = '_validator_%d' % (idx,)
            ctor_namespace[vname] = validator.validate.fn
            exc = validator.exception
            if isinstance(exc, type) and issubclass(exc, Exception):
                exc = exc()
            if isinstance(exc, Exception):
                ename = '_validator_exception_%d' % (idx,)
                ctor_namespace[ename] = exc
                raise_code = 'raise %s' % ename
            elif isinstance(exc, CallableSpecification):
                ename = '_validator_callable_%d' % (idx,)
                ctor_namespace[ename] = exc.fn
                raise_code = '%s(%s)' % (ename, ', '.join(exc.args))
            else:
                raise TypeError(exc)
            validators_code += self.VALIDATOR_CODE.format(
                validator_name=vname,
                validator_args=', '.join(validator.validate.args),
                validator_raise=raise_code
            )
        return validators_code

    COERCER_CODE = """
    {field_name} = {coercer_name}({coercer_args})
"""

    def build_coercers_code(
        self,
        field_names,
        coercers_by_field_name,
        unary_coercers_by_field_name,
        ctor_namespace
    ):
        both = set(coercers_by_field_name) & set(unary_coercers_by_field_name)
        if both:
            raise TypeError('Fields have both coercion options set', both)
        coercers_code = ''
        idx = 0
        for field_name, coercer in unary_coercers_by_field_name.items():
            cname = '_coercer_%d' % (idx,)
            ctor_namespace[cname] = coercer
            coercers_code += self.COERCER_CODE.format(
                coercer_name=cname,
                field_name=field_name,
                coercer_args=field_name,
            )
            idx += 1
        for field_name, coercer in coercers_by_field_name.items():
            spec = CallableSpecification.of(coercer)
            cname = '_coercer_%d' % (idx,)
            ctor_namespace[cname] = coercer
            coercers_code += self.COERCER_CODE.format(
                coercer_name=cname,
                field_name=field_name,
                coercer_args=', '.join(spec.args),
            )
            idx += 1
        return coercers_code

    POPULATOR_CODE = """
    if {field_name} is _NOT_SET:
        {field_name} = {populator_name}({populator_args})
"""

    def build_populators_code(self, field_names, populators_by_field_name, ctor_namespace):
        populators_code = ''
        given_fields = (set(field_names) - set(populators_by_field_name)) | set(['_cls'])
        populator_args_sort_map = dict((k, set()) for k in given_fields)
        for field_name, populator in populators_by_field_name.items():
            if field_name in populator.populator.args:
                raise ValueError('Populator %r has as self dependency on field %r' % (populator, field_name))
            populator_args_sort_map[field_name] = set(populator.populator.args)
        sorted_populators = [populators_by_field_name[field_name]
                             for lst in toposort(populator_args_sort_map)
                             for field_name in lst if field_name in populators_by_field_name]
        for idx, populator in enumerate(sorted_populators):
            pname = '_populator_%d' % (idx,)
            ctor_namespace[pname] = populator.populator.fn
            populators_code += self.POPULATOR_CODE.format(
                populator_name=pname,
                populator_args=', '.join(populator.populator.args),
                field_name=populator.field_name,
            )
        return populators_code

    def install_methods(self, dct, fields, repr_prefix=''):
        num_fields = len(fields)

        @classmethod
        def _make(cls, iterable, new=None, len=len):
            if new is None:
                new = cls.__new__
            result = new(cls, *tuple(iterable))
            if len(result) != num_fields:
                raise TypeError('Expected %d arguments, got %d' % (num_fields, len(result)))
            return result
        dct['_make'] = _make

        field_names = [field.name for field in fields]

        def _replace(self, **kwds):
            result = self._make(map(kwds.pop, field_names, self))
            if kwds:
                raise ValueError('Got unexpected field names: %r' % list(kwds))
            return result
        dct['_replace'] = _replace

        def _setdefault(self, **kwds):
            result = self._make(new_value if value == field.default else value
                                for field, value in zip(fields, self)
                                for new_value in (kwds.pop(field.name, value),))
            if kwds:
                raise ValueError('Got unexpected field names: %r' % list(kwds))
            return result
        dct['_setdefault'] = _setdefault

        repr_fmt = '(%s)' % (', '.join('{name}=%r'.format(name=name) for name in field_names),)

        def __repr__(self):
            return repr_prefix + self.__class__.__name__ + ('@%x' % (id(self),)) + repr_fmt % self
        dct.setdefault('__repr__', __repr__)

        dct.setdefault('__hash__', tuple.__hash__)

        def __eq__(self, other):
            return type(other) is type(self) and tuple.__eq__(self, other)
        dct.setdefault('__eq__', __eq__)

        def __ne__(self, other):
            return type(other) is not type(self) or tuple.__ne__(self, other)
        dct.setdefault('__ne__', __ne__)

        # def __lt__(self, other):
        #     return tuple.__lt__(self, other)
        # dct.setdefault('__lt__', __lt__)

        # def __le__(self, other):
        #     return tuple.__le__(self, other)
        # dct.setdefault('__le__', __le__)

        # def __gt__(self, other):
        #     return tuple.__gt__(self, other)
        # dct.setdefault('__gt__', __gt__)

        # def __ge__(self, other):
        #     return tuple.__ge__(self, other)
        # dct.setdefault('__ge__', __ge__)

        @property
        def __dict__(self):
            return RecordDict(zip(self._fields, self))
        dct.setdefault('__dict__', __dict__)

        def _asdict(self):
            return self.__dict__
        dct.setdefault('_asdict', _asdict)

        def _to_dict(self, for_key=False):
            return self._builder.to_dict(self, for_key=for_key)
        dct.setdefault('_to_dict', _to_dict)

        @classmethod
        def _from_dict(cls, obj):
            return self.from_dict(cls, obj)
        dct.setdefault('_from_dict', _from_dict)

        @classmethod
        def _to_optparse(cls, option_parser, **kwargs):
            return self.to_optparse(option_parser, **kwargs)
        dct.setdefault('_to_optparse', _to_optparse)

        @classmethod
        def _from_optparse(cls, options, **kwargs):
            return self.from_optparse(cls, options, **kwargs)
        dct.setdefault('_from_optparse', _from_optparse)

        @classmethod
        def _seal_type(cls):
            def __new__(_cls, *args, **kwargs):
                raise TypeError('Record type is sealed and may not be instantiated', _cls)
            cls.__new__ = __new__
        dct.setdefault('_seal_type', _seal_type)

    def to_dict(self, obj, for_key=False):
        if for_key and not getattr(type(obj), '_keyable', False):
            raise TypeError(obj)
        out = [] if for_key else {}
        for field in self.get_all_fields():
            val = getattr(obj, field.name)
            if field.type_specification is not None:
                val = field.type_specification.to_dict_item(val, for_key=for_key)
            if for_key:
                out.append(val)
            else:
                out[field.name] = val
        return tuple(out) if for_key else out

    def get_field(self, name):
        for field in self.get_all_fields():
            if field.name == name:
                return field
        raise KeyError(name)

    def from_dict(self, cls, obj):
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, (tuple, list)) and getattr(cls, '_keyable', False):
            if len(obj) != len(cls._fields):
                raise TypeError(cls._fields, obj)
            dct = dict(zip(cls._fields, obj))
        elif isinstance(obj, collections.Mapping):
            dct = dict(obj.items())
        elif len(cls._args) == 1:
            dct = {cls._args[0]: obj}
        else:
            raise TypeError(obj)
        kwargs = {}
        fields_with_populators = frozenset(self.get_all_populators_by_field_name().keys())
        required_missing = []
        for field in self.get_all_fields():
            try:
                val = dct.pop(field.name)
            except KeyError:
                if field.default is NOT_SET and field.new is NOT_SET and field.name not in fields_with_populators:
                    required_missing.append(field.name)
                continue
            if field.type_specification is not None:
                kwargs[field.name] = field.type_specification.from_dict_item(val)
            else:
                kwargs[field.name] = val
        if dct:
            raise KeyError('Unexpected keys', list(dct), cls)
        if required_missing:
            raise KeyError('Missing required keys', required_missing, cls)
        return cls(**kwargs)

    def to_optparse(self, option_parser, prefix='', default_option_group=None):
        import optparse
        option_groups = {}
        for field in self.get_all_fields():
            if field.group is not None:
                try:
                    option_group = option_groups[field.group]
                except KeyError:
                    group_doc = self.get_field_group_doc(field.group)
                    option_group = optparse.OptionGroup(
                        option_parser, group_doc if group_doc is not None else field.group)
                    option_parser.add_option_group(option_group)
                    option_groups[field.group] = option_group
                    add_option = option_groups[field.group].add_option
            elif default_option_group is not None:
                add_option = default_option_group.add_option
            else:
                add_option = option_parser.add_option
            kwargs = {}
            if field.type_specification is not None:
                kwargs.update(field.type_specification.optparse_kwargs)
            if field.doc is not None:
                kwargs['help'] = field.doc
            if field.default is not NOT_SET:
                kwargs['default'] = field.default
            option_name = '--' + prefix + (field.option_name or field.name.replace('_', '-'))
            add_option(option_name, **kwargs)

    def from_optparse(self, cls, options, prefix=''):
        fields_with_populators = frozenset(self.get_all_populators_by_field_name().keys())
        dct = {}
        for field in self.get_all_fields():
            val = getattr(options, prefix + (field.option_name or field.name))
            if val is None:
                if field.default is NOT_SET and field.new is NOT_SET and field.name not in fields_with_populators:
                    raise AttributeError('Option ' + prefix + field.name + ' is required')
            if field.type_specification is not None:
                type_spec = field.type_specification
                if isinstance(type_spec, CollectionTypeSpecification):
                    if isinstance(type_spec.col_spec, SimpleTypeSpecification):
                        val = type_spec.col_spec.type(val)
            dct[field.name] = val
        return cls(**dct)

    def install_pickling(self, name, dct, pickle_path, module_globals):
        if module_globals is not None:
            # For pickling to work, the __module__ variable needs to be set to the frame
            # where the named tuple is created.
            dct['__module__'] = module_globals.get('__name__', '__main__')

        dct['_pickle_path'] = pickle_path

        if pickle_path:
            if module_globals is None:
                raise RuntimeError('Cannot set pickle path without module globals')

            nested_class_getter_name = '__NESTED_CLASS_GETTER__%s_%s__' % (name, pickle_path.replace('.', '_'))
            if nested_class_getter_name in module_globals:
                raise NameError(nested_class_getter_name)
            split_pickle_path = pickle_path.split('.')

            def __call__(self, *t):
                obj = module_globals[split_pickle_path[0]]
                for part in split_pickle_path[1:]:
                    obj = getattr(obj, part)
                cls = getattr(obj, name)
                return cls(*t)
            NestedClassGetter = type(nested_class_getter_name, (object,), {'__call__': __call__})
            NestedClassGetter.__module__ = module_globals.get('__name__', '__main__')
            module_globals[nested_class_getter_name] = NestedClassGetter
            nested_class_getter = NestedClassGetter()

            def __reduce_ex__(self, protocol):
                return (nested_class_getter, tuple(self))
            dct.setdefault('__reduce_ex__', __reduce_ex__)

        else:
            def __getnewargs__(self):
                return tuple(self)
            dct.setdefault('__getnewargs__', __getnewargs__)

            def __getstate__(self):
                return None
            dct.setdefault('__getstate__', __getstate__)

    CTOR_CODE = """
def __new__(_cls, {ctor_arg_list}):
{abstract_check_code}
{populators_code}
{coercers_code}
{validators_code}
    return _tuple.__new__(_cls, ({arg_list}))
"""

    def build_doc(self, name, args, kwargs, pickle_path=None):
        lines = [(pickle_path + '.' if pickle_path else '') + name, '']
        for arg in args:
            lines.append('%s -> %s(%s)' % (arg.name, arg.doc + ' ' if arg.doc else '', arg.type))
        for kwarg in kwargs:
            lines.append('%s => %s(%s)' % (kwarg.name, kwarg.doc + ' ' if kwarg.doc else '', kwarg.type))
        return '\n'.join(lines)

    def build(
        self,
        name,
        abstract=False,
        verbose=False,
        asserted_validations=NOT_SET,
        pickle_path=None,
        module_globals=None,
        keyable=NOT_SET
    ):
        ctor_namespace = {
            '_NOT_SET': NOT_SET,
            '_tuple': tuple,
        }
        all_fields = list(self.get_all_fields())
        populators_by_field_name = self.get_all_populators_by_field_name()
        coercers_by_field_name = dict(
            (field.name, field.coerce) for field in all_fields if field.coerce is not NOT_SET)
        unary_coercers_by_field_name = dict(
            (field.name, field.coerce_) for field in all_fields if field.coerce_ is not NOT_SET)
        validators = list(self.get_all_type_validators(name)) + list(self.get_all_validators())

        args = []
        kwargs = []
        for field in all_fields:
            if field.name in populators_by_field_name:
                kwargs.append((field, '_NOT_SET'))
            elif field.default is not NOT_SET:
                dname = '_%s_default' % field.name
                ctor_namespace[dname] = field.default
                kwargs.append((field, dname))
            else:
                args.append(field)

        fields = args + list(list(zip(*kwargs))[0] if kwargs else [])
        field_names = [field.name for field in fields]
        if any(field_name.startswith('_') for field_name in field_names):
            raise NameError('Field names may not start with underscores: ' + repr(field_names))

        dct = self.dict.copy()
        dct['_fields'] = tuple(field_names)
        dct['_indices'] = dict(map(reversed, enumerate(field_names)))
        dct['_args'] = tuple(f.name for f in args)
        dct['_kwargs'] = tuple(kwargs)
        dct['_doc'] = self.build_doc(name, args, list(zip(*kwargs))[0] if kwargs else [], pickle_path=pickle_path)
        dct.setdefault('__slots__', ())
        if keyable is not NOT_SET:
            dct['_keyable'] = keyable

        if asserted_validations is not NOT_SET:
            dct['_asserted_validations'] = asserted_validations
        are_validations_enabled = self.are_validations_enabled(dct)

        abstract_check_code = self.build_abstract_check_code(
            name, abstract, dct)
        if are_validations_enabled:
            validators_code = self.build_validators_code(
                validators, ctor_namespace)
        else:
            validators_code = ''
        coercers_code = self.build_coercers_code(
            field_names, coercers_by_field_name, unary_coercers_by_field_name, ctor_namespace)
        populators_code = self.build_populators_code(
            field_names, populators_by_field_name, ctor_namespace)

        arg_list = repr(tuple(field_names)).replace("'", "")[1:-1]
        ctor_arg_list = ', '.join(
            [field.name for field in args] +
            ['%s=%s' % (field.name, dname2) for field, dname2 in kwargs])
        ctor_code = self.CTOR_CODE.format(
            arg_list=arg_list,
            ctor_arg_list=ctor_arg_list,
            abstract_check_code=abstract_check_code,
            populators_code=populators_code,
            coercers_code=coercers_code,
            validators_code=validators_code,
        )
        if verbose:
            print(ctor_code)
        exec_(ctor_code, ctor_namespace)
        dct['__new__'] = ctor_namespace['__new__']
        setattr(dct['__new__'], '__source__', ctor_code)

        self.install_methods(dct, fields, repr_prefix=(pickle_path + '.') if pickle_path else '')
        self.install_pickling(name, dct, pickle_path, module_globals)

        for idx, field in enumerate(fields):
            dct[field.name] = property(operator.itemgetter(idx), doc='Alias for field number %d' % (idx,))
        dct['_builder'] = self
        mcls = abc.ABCMeta if abstract else type
        cls = mcls(name, tuple(self.bases) + (Record,), dct)
        if abstract:
            cls.__metaclass__ = abc.ABCMeta
        return cls


class EmptyClass(object):
    pass

IGNORED_DICT_KEYS = frozenset(EmptyClass.__dict__.keys()) | frozenset(['_builder'])


def record(**build_kwargs):
    def inner(cls):
        module_globals = sys._getframe(1).f_globals
        try:
            builder = cls.__dict__['_builder']
        except KeyError:
            builder = RecordBuilder()
        names_by_placeholder = dict(
            (v, k) for k, v in cls.__dict__.items() if isinstance(v, NamePlaceholder))
        builder.resolve_name_placeholders(names_by_placeholder)
        for base in cls.__bases__:
            if base is not object:
                builder.add_base(base)
        dict_updates = dict(
            (k, v) for k, v in cls.__dict__.items()
            if not isinstance(v, NamePlaceholder) and k not in IGNORED_DICT_KEYS)
        builder.update_dict(dict_updates)
        return builder.build(cls.__name__, module_globals=module_globals, **build_kwargs)
    return inner


def record_fn(fn_name, named=False):
    def inner(*args, **kwargs):
        cls_dct = sys._getframe(1).f_locals
        try:
            builder = cls_dct['_builder']
        except KeyError:
            builder = cls_dct['_builder'] = RecordBuilder()
        if named:
            name_placeholder = ret = NamePlaceholder()
            args = (name_placeholder,) + args
        else:
            ret = None
        getattr(builder, fn_name)(*args, **kwargs)
        return ret
    return inner


def record_type_spec(cls):
    def inner(*args, **kwargs):
        return cls(*map(TypeSpecification.build, args), **kwargs)
    return inner


record.field = record_fn('add_field', named=True)
record.validate = record_fn('add_validator')
record.populate = record_fn('add_populator')
record.field_group_doc = record_fn('set_field_group_doc')
record.het_type = record_type_spec(HeterogeneousCollectionTypeSpecification)
record.all_type = record_type_spec(AllTypeSpecification)
record.type_specification_builder = TypeSpecification.builder
record.Record = Record
record.Builder = RecordBuilder
