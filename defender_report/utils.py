import itertools
import sys
import threading
import time
import logging

def configure_logging() -> None:
    """
    Configure the root logger with INFO level and a consistent timestamped format.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

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
