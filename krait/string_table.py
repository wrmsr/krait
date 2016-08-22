# -*- coding: utf-8 -*-
from __future__ import absolute_import

import ctypes
import mmap
import os
import struct
import sys

import json

from . import dyn
from . import libc
from . import six


MAX_HASH_GENERATION_ITERATIONS = dyn.Var(2**20)
MAX_INLINE_BYTES = dyn.Var(2**20)

ENCODING = 'utf-8'


def _maybe_encode(s, encoding=ENCODING):
    if isinstance(s, six.binary_type):
        return s
    else:
        return s.encode(encoding)

if sys.version_info[0] > 2:
    def _ord(o):
        return o
else:
    _ord = ord


FNV_32_KEY = 0x01000193


# Calculates a distinct hash function for a given string. Each value of the
# integer d results in a different hash value.
def fnv_32_hash(d, bs):
    __fnv_32_key = FNV_32_KEY
    __ord = _ord

    if d == 0:
        d = __fnv_32_key

    # Use the FNV algorithm from http://isthe.com/chongo/tech/comp/fnv/
    for b in bs:
        d = ((d * __fnv_32_key) ^ __ord(b)) & 0xffffffff

    return d


class HashGenerationFailure(Exception):
    pass


# Computes a minimal perfect hash table using the given python dictionary. It
# returns a tuple (G, V). G and V are both arrays. G contains the intermediate
# table of values needed to compute the index of the value in V. V contains the
# values of the dictionary.
# Source: http://stevehanov.ca/blog/index.php?id=119
# TODO(wtimoney): disk-back this.
def create_minimal_perfect_hash(dct):
    if not dct:
        raise ValueError(dct)

    max_iterations = MAX_HASH_GENERATION_ITERATIONS()

    def run():
        size = len(dct)

        # Step 1: Place all of the keys into buckets
        buckets = [[] for i in range(size)]
        gs = [0] * size
        vs = [None] * size

        for key in dct.keys():
            buckets[fnv_32_hash(0, key) % size].append(key)

        # Step 2: Sort the buckets and process the ones with the most items first.
        buckets.sort(key=len, reverse=True)

        for b in six.moves.xrange(size):
            bucket = buckets[b]
            if len(bucket) <= 1:
                break

            d = 1
            item = 0
            slots = []

            # Repeatedly try different values of d until we find a hash function
            # that places all items in the bucket into free slots
            while item < len(bucket):
                slot = fnv_32_hash(d, bucket[item]) % size

                if vs[slot] is not None or slot in slots:
                    d += 1
                    if d >= max_iterations:
                        raise HashGenerationFailure()
                    item = 0
                    slots = []

                else:
                    slots.append(slot)
                    item += 1

            gs[fnv_32_hash(0, bucket[0]) % size] = d

            for i in six.moves.xrange(len(bucket)):
                vs[slots[i]] = dct[bucket[i]]

        # Only buckets with 1 item remain. Process them more quickly by directly
        # placing them into a free slot. Use a negative value of d to indicate
        # this.
        freelist = [i for i in six.moves.xrange(size) if vs[i] is None]

        for b in six.moves.xrange(b, size):
            bucket = buckets[b]
            if len(bucket) == 0:
                break

            slot = freelist.pop()

            # We subtract one to ensure it's negative even if the zeroeth slot was
            # used.
            gs[fnv_32_hash(0, bucket[0]) % size] = -slot - 1

            vs[slot] = dct[bucket[0]]

        return gs, vs

    return run()


# Look up a value in the hash table, defined by G and V.
def lookup_minimal_perfect_hash(gs, vs, key):
    d = gs[fnv_32_hash(0, key) % len(gs)]

    if d < 0:
        return vs[-d - 1]

    return vs[fnv_32_hash(d, key) % len(vs)]


def verify_minimal_perfect_hash(gs, vs, dct):
    for k, v in six.iteritems(dct):
        v_ = lookup_minimal_perfect_hash(gs, vs, k)

        if v != v_:
            raise ValueError((k, v, v_))


class StructSection(object):

    def __init__(self, base, dtype, offset, length):
        super(StructSection, self).__init__()
        self._base = base
        self._dtype = dtype
        self._offset = offset
        self._length = length
        self._ptr = ctypes.cast(base + offset, ctypes.POINTER(dtype * (length // ctypes.sizeof(dtype)))).contents

    def __getitem__(self, idx):
        return self._ptr[idx]

    def __len__(self):
        return self._length // ctypes.sizeof(self._dtype)

    @classmethod
    def write(cls, bin_file, fmt, values):
        offset = bin_file.tell()
        for v in values:
            bin_file.write(struct.pack(fmt, v))
        length = bin_file.tell() - offset
        return {
            'offset': offset,
            'length': length,
        }


class StringSection(object):

    def __init__(self, base, offsets, bytes):
        super(StringSection, self).__init__()
        self._offsets = StructSection(base, ctypes.c_uint32, **offsets)
        self._bytes = StructSection(base, ctypes.c_uint32, **bytes)

    def __getitem__(self, idx):
        offset = self._offsets[idx]
        ptr = ctypes.cast(self._bytes._base + self._bytes._offset + offset, ctypes.c_char_p)
        return ptr.value.decode(ENCODING)

    def __len__(self):
        return len(self._offsets) - 1

    @classmethod
    def write(cls, bin_file, strs):
        bytes_offset = bin_file.tell()
        offsets = []
        dct = {}
        for i, s in enumerate(strs):
            if s in dct:
                offsets.append(dct[s])
            else:
                offset = bin_file.tell() - bytes_offset
                dct[s] = offset
                offsets.append(offset)
                if s:
                    if b'\0' in s:
                        raise ValueError(s)
                    bin_file.write(s)
                bin_file.write(b'\0')
        bytes_length = bin_file.tell() - bytes_offset
        offsets_info = StructSection.write(bin_file, 'I', offsets)
        return {
            'offsets': offsets_info,
            'bytes': {
                'offset': bytes_offset,
                'length': bytes_length,
            }
        }


class HashStringTable(object):

    def __init__(self, base, gs, vs, keys, values):
        super(HashStringTable, self).__init__()
        self._gs = StructSection(base, ctypes.c_int32, **gs)
        self._vs = StructSection(base, ctypes.c_int32, **vs)
        self._keys = StringSection(base, **keys)
        self._values = StringSection(base, **values)

    def __getitem__(self, key):
        encoded_key = _maybe_encode(key)
        idx = lookup_minimal_perfect_hash(self._gs, self._vs, encoded_key)
        table_key = self._keys[idx]
        if table_key != key:
            raise KeyError(key)
        return self._values[idx]

    @classmethod
    def _generate(cls, dct):
        keys, values = zip(*sorted(dct.items()))
        value_idxs_by_key = dict(map(reversed, enumerate(keys)))
        gs, vs = create_minimal_perfect_hash(value_idxs_by_key)
        verify_minimal_perfect_hash(gs, vs, value_idxs_by_key)
        return gs, vs, keys, values

    @classmethod
    def write(cls, bin_file, dct):
        gs, vs, keys, values = cls._generate(dct)
        gs_info = StructSection.write(bin_file, 'i', gs)
        vs_info = StructSection.write(bin_file, 'i', vs)
        keys_info = StringSection.write(bin_file, keys)
        values_info = StringSection.write(bin_file, values)
        return {
            'gs': gs_info,
            'vs': vs_info,
            'keys': keys_info,
            'values': values_info,
        }


class InlineStringTable(object):

    def __init__(self, base, dct):
        super(InlineStringTable, self).__init__()
        self._dct = dct

    def __getitem__(self, key):
        return self._dct[key]

    @classmethod
    def write(cls, bin_file, dct):
        return {'dct': dct}


class StringTable(object):

    def __init__(self, base, hash=None, inline=None):
        super(StringTable, self).__init__()
        if hash is not None and inline is not None:
            raise TypeError()
        elif hash is not None:
            self._impl = HashStringTable(base, **hash)
        elif inline is not None:
            self._impl = InlineStringTable(base, **inline)
        else:
            raise TypeError()

    def __getitem__(self, key):
        return self._impl[key]

    @classmethod
    def write(cls, bin_file, dct, max_inline_bytes=2**20):
        encoded_dct = dict(map(_maybe_encode, t) for t in six.iteritems(dct))
        try:
            return {'hash': HashStringTable.write(bin_file, encoded_dct)}
        except HashGenerationFailure:
            if len(json.dumps(dct)) >= MAX_INLINE_BYTES():
                raise ValueError()
            return {'inline': InlineStringTable.write(bin_file, dct)}


class StringTableFile(object):

    def __init__(self, bin_file_path):
        super(StringTableFile, self).__init__()
        self._bin_file_path = bin_file_path

    bin_file_path = property(lambda self: self._bin_file_path)
    info_file_path = property(lambda self: self.bin_file_path + '.info')

    @property
    def info(self):
        with open(self.info_file_path, 'r') as info_file:
            return json.loads(info_file.read())

    def __enter__(self):
        info = self.info
        self._fd = os.open(self.bin_file_path, os.O_RDONLY)
        self._size = os.fstat(self._fd).st_size
        self._base = libc.mmap(0, self._size, mmap.PROT_READ, mmap.MAP_SHARED, self._fd, 0)
        self._tables = dict(
            (name, StringTable(self._base, **table_info))
            for name, table_info in info.items())
        return self

    def __exit__(self, et, e, tb):
        libc.munmap(self._base, self._size)
        os.close(self._fd)
        del self._tables

    def __getitem__(self, name):
        return self._tables[name]

    @classmethod
    def write(cls, bin_file_path, dct):
        with open(bin_file_path, 'wb') as bin_file:
            info = dict(
                (name, StringTable.write(bin_file, table_dct))
                for name, table_dct in dct.items())
        with open(bin_file_path + '.info', 'w') as info_file:
            info_file.write(json.dumps(info, indent=4))
