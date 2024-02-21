import os
import threading


class ProgressPercentage(object):
    def __init__(self, filename, log_file):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._log_file = log_file

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            with open(self._log_file, "a") as f:
                f.write(
                    "\r%s  %s / %s  (%.2f%%)\n"
                    % (self._filename, self._seen_so_far, self._size, percentage)
                )
