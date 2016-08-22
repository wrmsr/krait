# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os

from krait import string_table


def test_no_collision(tmpdir):
    dct = {
        'things': {
            'abc': 'def',
            'ghi': 'jkl',
            'mno': 'pqr',
            'stu': 'pqr',
            'xyz': 'def',
            'bullshit': 'stuff',
        }
    }

    pth = os.path.join(str(tmpdir), 'stuff.bin')

    string_table.StringTableFile.write(pth, dct)
    with string_table.StringTableFile(pth) as stf:
        for k, v in dct['things'].items():
            assert stf['things'][k] == v


def test_collision(tmpdir):
    dct = {
        'things': {
            'abc': 'def',
            'ghi': 'jkl',
            'mno': 'pqr',
            'stu': 'pqr',
        }
    }

    pth = os.path.join(str(tmpdir), 'stuff.bin')

    string_table.StringTableFile.write(pth, dct)
    with string_table.StringTableFile(pth) as stf:
        for k, v in dct['things'].items():
            assert stf['things'][k] == v
