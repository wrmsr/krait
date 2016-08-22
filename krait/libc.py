# -*- coding: utf-8 -*-
from __future__ import absolute_import

import ctypes
import signal
import sys


LINUX_PLATFORMS = ('linux', 'linux2')
DARWIN_PLATFORMS = ('darwin',)

LINUX = False
DARWIN = False

if sys.platform in LINUX_PLATFORMS:
    libc = ctypes.CDLL('libc.so.6')
    LINUX = True

elif sys.platform in DARWIN_PLATFORMS:
    libc = ctypes.CDLL('/usr/lib/libc.dylib')
    DARWIN = True

else:
    raise EnvironmentError('Unsupported platform')


# void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset);
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_size_t
]
mmap = libc.mmap

PROT_NONE = 0x0  # Page can not be accessed.
PROT_READ = 0x1  # Page can be read.
PROT_WRITE = 0x2  # Page can be written.
PROT_EXEC = 0x4  # Page can be executed.

if LINUX:
    MAP_SHARED = 0x01  # Share changes.
    MAP_PRIVATE = 0x02  # Changes are private.
    MAP_ANONYMOUS = 0x20  # Don't use a file.
    MAP_GROWSDOWN = 0x00100  # Stack-like segment.
    MAP_DENYWRITE = 0x00800  # ETXTBSY
    MAP_EXECUTABLE = 0x01000  # Mark it as an executable.
    MAP_LOCKED = 0x02000  # Lock the mapping.
    MAP_NORESERVE = 0x04000  # Don't check for reservations.
    MAP_POPULATE = 0x08000  # Populate (prefault) pagetables.
    MAP_NONBLOCK = 0x10000  # Do not block on IO.
    MAP_STACK = 0x20000  # Allocation is for a stack.
    MAP_HUGETLB = 0x40000  # create a huge page mapping

elif DARWIN:
    MAP_SHARED = 0x0001  # [MF|SHM] share changes
    MAP_PRIVATE = 0x0002  # [MF|SHM] changes are private
    MAP_FIXED = 0x0010  # [MF|SHM] interpret addr exactly
    MAP_RENAME = 0x0020  # Sun: rename private pages to file
    MAP_NORESERVE = 0x0040  # Sun: don't reserve needed swap area
    MAP_RESERVED0080 = 0x0080  # previously unimplemented MAP_INHERIT
    MAP_NOEXTEND = 0x0100  # for MAP_FILE, don't change file size
    MAP_HASSEMAPHORE = 0x0200  # region may contain semaphores
    MAP_NOCACHE = 0x0400  # don't cache pages for this mapping
    MAP_JIT = 0x0800  # Allocate a region that will be used for JIT purposes
    MAP_FILE = 0x0000  # map from file (default)
    MAP_ANON = 0x1000  # allocated from memory, swap space


# int munmap(void *addr, size_t length);
libc.munmap.restype = ctypes.c_int
libc.munmap.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t
]
munmap = libc.munmap


# int mprotect(const void *addr, size_t len, int prot);
libc.mprotect.restype = ctypes.c_int
libc.mprotect.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int
]
mprotect = libc.mprotect

if LINUX:
    PROT_GROWSDOWN = 0x01000000  # Extend change to start of growsdown vma (mprotect only).
    PROT_GROWSUP = 0x02000000  # Extend change to start of growsup vma (mprotect only).


# int msync(void *addr, size_t length, int flags);
libc.msync.restype = ctypes.c_int
libc.msync.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int
]
msync = libc.msync

MS_ASYNC = 1  # Sync memory asynchronously.
MS_INVALIDATE = 2  # Invalidate the caches.

if LINUX:
    MS_SYNC = 4  # Synchronous memory sync.

elif DARWIN:
    MS_SYNC = 0x0010  # [MF|SIO] msync synchronously


# int mlock(const void *addr, size_t len);
libc.mlock.restype = ctypes.c_int
libc.mlock.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t
]
mlock = libc.mlock


# int munlock(const void *addr, size_t len);
libc.munlock.restype = ctypes.c_int
libc.munlock.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t
]
munlock = libc.munlock


# int mlockall(int flags);
libc.mlockall.restype = ctypes.c_int
libc.mlockall.argtypes = [ctypes.c_int]
mlockall = libc.mlockall


# int munlockall(void);
libc.munlockall.restype = ctypes.c_int
libc.munlockall.argtypes = []
munlockall = libc.munlockall

MCL_CURRENT = 1  # Lock all currently mapped pages.
MCL_FUTURE = 2  # Lock all additions to address space.


if LINUX:
    # void *mremap(void *old_address, size_t old_size, size_t new_size, int flags);
    libc.mremap.restype = ctypes.c_void_p
    libc.mremap.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_int
    ]
    mremap = libc.mremap

    MREMAP_MAYMOVE = 1
    MREMAP_FIXED = 2


if LINUX:
    # ssize_t splice(int fd_in, loff_t *off_in, int fd_out, loff_t *off_out, size_t len, unsigned int flags);
    libc.splice.restype = ctypes.c_size_t
    libc.splice.argtypes = [
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_size_t,
        ctypes.c_uint
    ]
    splice = libc.splice

    SPLICE_F_MOVE = 1  # Move pages instead of copying.
    SPLICE_F_NONBLOCK = 2  # Don't block on the pipe splicing (but we may still block on the fd we splice from/to).
    SPLICE_F_MORE = 4  # Expect more data.
    SPLICE_F_GIFT = 8  # Pages passed in are a gift.


# int raise(int sig);
libc._raise = libc['raise']
libc._raise.restype = ctypes.c_int
libc._raise.argtypes = [ctypes.c_int]
_raise = libc._raise


def sigtrap():
    libc._raise(signal.SIGTRAP)


class Malloc(object):

    def __init__(self, sz):
        self.sz = sz
        self.base = 0

    def __enter__(self):
        self.base = libc.malloc(self.sz)

    def __exit__(self, et, e, tb):
        libc.free(self.base)
        self.base = 0

    def __int__(self):
        return int(self.base)

    def __long__(self):
        return long(self.base)


# typedef struct {
#     PyObject_HEAD
#     char *      data;
#     size_t      size;
#     size_t      pos;    /* relative to offset */
#     size_t      offset;

# #ifdef MS_WINDOWS
#     HANDLE      map_handle;
#     HANDLE      file_handle;
#     char *      tagname;
# #endif

# #ifdef UNIX
#     int fd;
# #endif

#     access_mode access;
# } mmap_object;
