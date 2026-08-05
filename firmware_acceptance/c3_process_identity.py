"""C3-only process command identity predicates."""
from __future__ import annotations

import os
import re


def lane_controller_command_matches(shape: str, command_line: str, invocation_path: str) -> bool:
    """Accept exact Popen identity or a losslessly parsed Windows redirector argv."""
    if shape == "same-process":
        return True
    if shape != "direct-venv-redirector" or os.name != "nt" or not isinstance(command_line, str) or not command_line or not isinstance(invocation_path, str) or not invocation_path:
        return False
    try:
        import ctypes
        argc = ctypes.c_int()
        argv_type = ctypes.POINTER(ctypes.c_wchar_p)
        shell32 = ctypes.WinDLL("shell32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        shell32.CommandLineToArgvW.argtypes = (ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_int))
        shell32.CommandLineToArgvW.restype = argv_type
        kernel32.LocalFree.argtypes = (ctypes.c_void_p,)
        kernel32.LocalFree.restype = ctypes.c_void_p
        argv_ptr = shell32.CommandLineToArgvW(command_line, ctypes.byref(argc))
        if not argv_ptr:
            return False
        try:
            argv = [argv_ptr[index] for index in range(argc.value)]
        finally:
            kernel32.LocalFree(ctypes.cast(argv_ptr, ctypes.c_void_p))
    except (AttributeError, OSError, ValueError):
        return False
    executable = os.path.basename(argv[0]).casefold() if len(argv) == 4 and isinstance(argv[0], str) else ""
    return (re.fullmatch(r"python(?:3(?:[._]?\d+)?)?\.exe", executable) is not None
            and argv[1:] == ["-m", "orchestrator_harness.lane_controller", invocation_path])
