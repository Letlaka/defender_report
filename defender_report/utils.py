import itertools
import logging
import os
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from typing import Optional


def configure_logging(
    log_file_path: Optional[str] = None, level: int = logging.INFO
) -> None:
    """
    Configure root logger to:
      • Stream to STDOUT
      • Optionally write to a rotating file
    """
    # 1) Remove any pre-existing handlers
    root_logger = logging.getLogger()
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    root_logger.setLevel(level)

    # 2) Create a shared formatter
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    # 3) Console (CLI) handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 4) File handler, if requested
    if log_file_path:
        # ensure directory exists
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,  # keep last 5 files
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


class Spinner:
    """
    Context manager that displays a terminal spinner while a long-running
    operation is in progress.
    """

    def __init__(self, message: str = "Working"):
        self.message = message
        self._spinner_cycle = itertools.cycle(["|", "/", "-", "\\"])
        self._stop_event = threading.Event()

    def __enter__(self) -> "Spinner":
        threading.Thread(target=self._spin, daemon=True).start()
        return self

    def _spin(self) -> None:
        while not self._stop_event.is_set():
            sys.stdout.write(f"\r{self.message} {next(self._spinner_cycle)}")
            sys.stdout.flush()
            time.sleep(0.1)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._stop_event.set()
        sys.stdout.write("\r" + " " * (len(self.message) + 2) + "\r")
        sys.stdout.flush()
