"""Read macOS process CPU accounting via the installed SDK's rusage_info_v0."""

import ctypes


class Usage(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64) for name in (
            "user_ticks", "system_ticks", "package_idle_wakeups", "interrupt_wakeups",
            "pageins", "wired_size", "resident_size", "physical_footprint",
            "start_abstime", "exit_abstime",
        )
    ]


LIBPROC = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
LIBPROC.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
LIBPROC.proc_pid_rusage.restype = ctypes.c_int


class Timebase(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


LIBSYSTEM = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
LIBSYSTEM.mach_timebase_info.argtypes = [ctypes.POINTER(Timebase)]
LIBSYSTEM.mach_timebase_info.restype = ctypes.c_int
TIMEBASE = Timebase()
assert LIBSYSTEM.mach_timebase_info(ctypes.byref(TIMEBASE)) == 0
SECONDS_PER_TICK = TIMEBASE.numer / TIMEBASE.denom / 1e9


def cpu_usage(pid):
    usage = Usage()
    if LIBPROC.proc_pid_rusage(pid, 0, ctypes.byref(usage)):
        raise OSError(ctypes.get_errno(), "proc_pid_rusage failed")
    return {
        "user_seconds": usage.user_ticks * SECONDS_PER_TICK,
        "system_seconds": usage.system_ticks * SECONDS_PER_TICK,
        "package_idle_wakeups": usage.package_idle_wakeups,
        "interrupt_wakeups": usage.interrupt_wakeups,
    }
