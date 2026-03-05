"""
Utility for PyInstaller compatibility.
Provides the correct base directory whether running from source or frozen exe.
"""
import os
import sys


def get_app_dir() -> str:
    """
    Return the application directory.
    - When running from source: the directory containing this file.
    - When frozen by PyInstaller (--onedir): the directory containing the .exe
    """
    if getattr(sys, "frozen", False):
        # PyInstaller --onedir: sys._MEIPASS points to _internal,
        # but the exe is in the parent folder. However for --onedir,
        # the data files are relative to the exe location.
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))
