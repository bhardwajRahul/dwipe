"""
LsblkMonitor - Background thread for monitoring block device changes
"""
import os
import threading
import subprocess


class LsblkMonitor:
    """Background monitor that checks for block device changes and runs lsblk"""

    def __init__(self, check_interval=0.2):
        """
        Initialize the lsblk monitor.

        Args:
            check_interval: How often to check for changes (seconds)
        """
        self.check_interval = check_interval
        self.lsblk_str = ""
        self._lock = threading.Lock()
        self._thread = None
        self._stop_event = threading.Event()
        self.last_fingerprint = None

    def start(self):
        """Start the background monitoring thread"""
        if self._thread is not None and self._thread.is_alive():
            return  # Already running

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the background monitoring thread"""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def check_for_changes(self):
        """
        Check if block devices or partitions have changed.

        Returns:
            True if changes detected, False otherwise
        """
        try:
            # 1. Quickest check: Does the list of block devices match?
            # This catches "Forget" (DEL) and "Scan" (!) events immediately.
            current_devs = os.listdir('/sys/class/block')

            # 2. Secondary check: Do the partition sizes/counts match?
            with open('/proc/partitions', 'r', encoding='utf-8') as f:
                current_parts = f.read()

            # Create a combined "Fingerprint"
            fingerprint = f"{len(current_devs)}|{current_parts}"

            if fingerprint != self.last_fingerprint:
                self.last_fingerprint = fingerprint
                return True

        except Exception:  # pylint: disable=broad-exception-caught
            # If we can't read /sys or /proc, default to True
            # so we don't get stuck with a blank screen.
            return True
        return False

    def _run_lsblk(self):
        """
        Run lsblk and capture output in JSON format matching DeviceInfo.parse_lsblk requirements.

        Returns:
            JSON output string from lsblk command
        """
        try:
            result = subprocess.run(
                ['lsblk', '-J', '--bytes', '-o',
                 'NAME,MAJ:MIN,FSTYPE,TYPE,LABEL,PARTLABEL,FSUSE%,SIZE,MOUNTPOINTS,UUID,PARTUUID,SERIAL'],
                capture_output=True,
                text=True,
                timeout=5.0,
                check=False
            )
            return result.stdout
        except Exception:  # pylint: disable=broad-exception-caught
            return ""  # Return empty string on error

    def _monitor_loop(self):
        """Background thread loop that monitors for changes"""
        while not self._stop_event.is_set():
            if self.check_for_changes():
                # Changes detected, run lsblk
                lsblk_output = self._run_lsblk()

                # Store the result in a thread-safe manner
                with self._lock:
                    self.lsblk_str = lsblk_output

            # Sleep for the check interval
            self._stop_event.wait(self.check_interval)

    def get_and_clear(self):
        """
        Get the latest lsblk output and clear it.

        Returns:
            String containing lsblk output, or empty string if no new data
        """
        with self._lock:
            result = self.lsblk_str
            self.lsblk_str = ""
            return result

    def peek(self):
        """
        Get the latest lsblk output without clearing it.

        Returns:
            String containing lsblk output, or empty string if no new data
        """
        with self._lock:
            return self.lsblk_str
