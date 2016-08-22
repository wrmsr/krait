#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import sys

import setuptools


if sys.version_info[0] > 2:
    try:
        exec_ = __builtins__['exec']
    except TypeError:
        exec_ = getattr(__builtins__, 'exec')
else:
    def exec_(_code_, _globals_=None, _locals_=None):
        if _globals_ is None:
            frame = sys._getframe(1)
            _globals_ = frame.f_globals
            if _locals_ is None:
                _locals_ = frame.f_locals
            del frame
        elif _locals_ is None:
            _locals_ = _globals_
            exec("""exec _code_ in _globals_, _locals_""")


BASE_DIR = os.path.dirname(__file__)
ABOUT = {}
with open(os.path.join(BASE_DIR, 'krait', '__about__.py')) as f:
    exec_(f.read(), ABOUT)


setuptools.setup(
    name=ABOUT['__title__'],
    version=ABOUT['__version__'],
    description=ABOUT['__description__'],
    author=ABOUT['__author__'],
    url=ABOUT['__url__'],
    license=ABOUT['__license__'],

    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],

    packages=setuptools.find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    include_package_data=True,
    entry_points={},

    install_requires=[],
    extra_requires={
        'yaml': ['PyYAML'],
    },
)
