# -*- coding: utf-8 -*-
from __future__ import absolute_import

import ctypes


libc = ctypes.CDLL('libc.so.6')

# int prctl(int option, unsigned long arg2, unsigned long arg3, unsigned long arg4, unsigned long arg5);
libc.prctl.restype = ctypes.c_int
libc.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]

prctl = libc.prctl

# Values to pass as first argument to prctl()

PR_SET_PDEATHSIG = 1  # Second arg is a signal
PR_GET_PDEATHSIG = 2  # Second arg is a ptr to return the signal

# Get/set current->mm->dumpable
PR_GET_DUMPABLE = 3
PR_SET_DUMPABLE = 4

# Get/set unaligned access control bits (if meaningful)
PR_GET_UNALIGN = 5
PR_SET_UNALIGN = 6
PR_UNALIGN_NOPRINT = 1  # silently fix up unaligned user accesses
PR_UNALIGN_SIGBUS = 2  # generate SIGBUS on unaligned user access

# Get/set whether or not to drop capabilities on setuid() away from
# uid 0 (as per security/commoncap.c)
PR_GET_KEEPCAPS = 7
PR_SET_KEEPCAPS = 8

# Get/set floating-point emulation control bits (if meaningful)
PR_GET_FPEMU = 9
PR_SET_FPEMU = 10
PR_FPEMU_NOPRINT = 1  # silently emulate fp operations accesses
PR_FPEMU_SIGFPE = 2  # don't emulate fp operations, send SIGFPE instead

# Get/set floating-point exception mode (if meaningful)
PR_GET_FPEXC = 11
PR_SET_FPEXC = 12
PR_FP_EXC_SW_ENABLE = 0x80  # Use FPEXC for FP exception enables
PR_FP_EXC_DIV = 0x010000  # floating point divide by zero
PR_FP_EXC_OVF = 0x020000  # floating point overflow
PR_FP_EXC_UND = 0x040000  # floating point underflow
PR_FP_EXC_RES = 0x080000  # floating point inexact result
PR_FP_EXC_INV = 0x100000  # floating point invalid operation
PR_FP_EXC_DISABLED = 0  # FP exceptions disabled
PR_FP_EXC_NONRECOV = 1  # async non-recoverable exc. mode
PR_FP_EXC_ASYNC = 2  # async recoverable exception mode
PR_FP_EXC_PRECISE = 3  # precise exception mode

# Get/set whether we use statistical process timing or accurate timestamp
# process timing

PR_SET_NAME = 15  # Set process name
PR_GET_NAME = 16  # Get process name

# Get/set process endian
PR_GET_ENDIAN = 19
PR_SET_ENDIAN = 20
PR_ENDIAN_BIG = 0
PR_ENDIAN_LITTLE = 1  # True little endian mode
PR_ENDIAN_PPC_LITTLE = 2  # "PowerPC" pseudo little endian

# Get/set process seccomp mode
PR_GET_SECCOMP = 21
PR_SET_SECCOMP = 22

# Get/set the capability bounding set (as per security/commoncap.c)
PR_CAPBSET_READ = 23
PR_CAPBSET_DROP = 24

# Get/set the process' ability to use the timestamp counter instruction
PR_GET_TSC = 25
PR_SET_TSC = 26
PR_TSC_ENABLE = 1  # allow the use of the timestamp counter
PR_TSC_SIGSEGV = 2  # throw a SIGSEGV instead of reading the TSC

# Get/set securebits (as per security/commoncap.c)
PR_GET_SECUREBITS = 27
PR_SET_SECUREBITS = 28

# Get/set the timerslack as used by poll/select/nanosleep
# A value of 0 means "use default"

PR_SET_TIMERSLACK = 29
PR_GET_TIMERSLACK = 30

PR_TASK_PERF_EVENTS_DISABLE = 31
PR_TASK_PERF_EVENTS_ENABLE = 32

# Set early/late kill mode for hwpoison memory corruption.
# This influences when the process gets killed on a memory corruption.
PR_MCE_KILL = 33
PR_MCE_KILL_CLEAR = 0
PR_MCE_KILL_SET = 1

PR_MCE_KILL_LATE = 0
PR_MCE_KILL_EARLY = 1
PR_MCE_KILL_DEFAULT = 2

PR_MCE_KILL_GET = 34
