# -*- coding: utf-8 -*-
"""https://gist.github.com/enaeseth/844388"""
from __future__ import absolute_import

import yaml
import yaml.constructor


try:
    from collections import OrderedDict
except ImportError:
    try:
        from ordereddict import OrderedDict
    except ImportError:
        from .ordereddict import OrderedDict


class OrderedDictYAMLLoader(yaml.Loader):
    """A YAML loader that loads mappings into ordered dictionaries."""

    def __init__(self, *args, **kwargs):
        super(OrderedDictYAMLLoader, self).__init__(*args, **kwargs)

        self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError(
                None, None, 'expected a mapping node, but found %s' % node.id, node.start_mark)

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError as exc:
                raise yaml.constructor.ConstructorError(
                    'while constructing a mapping',
                    node.start_mark,
                    'found unacceptable key (%s)' % exc,
                    key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


def load(stream):
    return yaml.load(stream, OrderedDictYAMLLoader)


def load_all(stream):
    return yaml.load_all(stream, OrderedDictYAMLLoader)


def safe_load(stream):
    return yaml.safe_load(stream, OrderedDictYAMLLoader)


def safe_load_all(stream):
    return yaml.safe_load_all(stream, OrderedDictYAMLLoader)


if __name__ == '__main__':
    import textwrap

    sample = """
    one:
        two: fish
        red: fish
        blue: fish
    two:
        a: yes
        b: no
        c: null
    """

    data = load(textwrap.dedent(sample))
    print(data)

    data = yaml.load(textwrap.dedent(sample))
    print(data)
