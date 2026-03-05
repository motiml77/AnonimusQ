"""
AnonimousQ – Desktop launcher
Starts Flask in a background thread, then opens Microsoft Edge (or Chrome)
in --app mode: a clean window with no browser UI (no address bar, no tabs).
Closing the window does NOT kill Flask – the process exits when run.py exits.
"""
import ctypes
import threading
import time
import subprocess
import sys
import os
import urllib.request
import logging

# ── Make sure imports work when run from any directory ──────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app as flask_app  # our Flask application

PORT = 5000
URL  = f"http://127.0.0.1:{PORT}/"


def _start_flask():
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)
    flask_app.app.run(host="127.0.0.1", port=PORT, use_reloader=False, threaded=True)


def _wait_for_flask(timeout=10):
    for _ in range(timeout * 10):
        try:
            urllib.request.urlopen(URL, timeout=0.2)
            return True
        except Exception:
            time.sleep(0.1)
    return False


def _find_browser():
    """Return (executable, args) for Edge or Chrome app-mode, or None."""
    candidates = [
        # Microsoft Edge (built-in on Windows 10/11)
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        # Google Chrome
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def _window_geometry():
    """Return (width, height, x, y) for an 80%-of-screen window centred on screen."""
    try:
        # Ask Windows for the true physical pixel counts (DPI-aware)
        user32 = ctypes.windll.user32
        user32.SetProcessDPIAware()
        screen_w = user32.GetSystemMetrics(0)
        screen_h = user32.GetSystemMetrics(1)
    except Exception:
        screen_w, screen_h = 1920, 1080   # safe fallback

    win_w = int(screen_w * 0.8)
    win_h = int(screen_h * 0.8)
    win_x = (screen_w - win_w) // 2
    win_y = (screen_h - win_h) // 2
    return win_w, win_h, win_x, win_y


def main():
    # 1. Start Flask
    t = threading.Thread(target=_start_flask, daemon=True)
    t.start()

    # 2. Wait until it's ready
    if not _wait_for_flask():
        print("Flask failed to start", file=sys.stderr)
        sys.exit(1)

    # 3. Open app window
    browser = _find_browser()
    if browser:
        win_w, win_h, win_x, win_y = _window_geometry()
        proc = subprocess.Popen([
            browser,
            f"--app={URL}",
            f"--window-size={win_w},{win_h}",
            f"--window-position={win_x},{win_y}",
            "--no-first-run",
            "--disable-extensions",
            "--disable-default-apps",
            f"--user-data-dir={os.path.join(os.path.dirname(__file__), 'data', 'browser-profile')}",
        ])
        proc.wait()   # block until the window is closed
    else:
        # Fallback: regular browser tab
        import webbrowser
        webbrowser.open(URL)
        t.join()      # keep Flask alive


if __name__ == "__main__":
    main()
