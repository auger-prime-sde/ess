"""
   Determine thread ID as reported by e.g. 'ps -eLf'
"""

import os
import ctypes

syscall = ctypes.cdll.LoadLibrary('libc.so.6').syscall

arch = os.uname()[4]
if arch == 'x86_64':
    # /usr/include/x86_64-linux-gnu/asm/unistd_64.h
    SYS_gettid = 186
elif arch == 'armv7l':
    # RPi: /usr/include/arm-linux-gnueabihf/asm/unistd-common.h
    SYS_gettid = 224
else:
    raise KeyError('Unknown architecture')
