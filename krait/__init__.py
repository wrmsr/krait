# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import record  # noqa

from . import six  # noqa

if len(six.string_types) == 1:
    @record.TypeSpecification.builder(lambda obj: obj is six.string_types, first=True)
    def _six_string_types_tsb(obj):
        return record.TypeSpecification.build(obj[0])

from . import frozendict  # noqa

record.MAPPING_COLLECTION_TYPES.append(frozendict.frozendict)

from .common import *  # noqa

from . import check  # noqa
from . import dyn  # noqa
from . import ordereddict  # noqa
