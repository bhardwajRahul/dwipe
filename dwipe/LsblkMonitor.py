"""
DeviceChangeMonitor - Background thread for monitoring block device changes
"""
import os
import threading


class LsblkMonitor:
    """Background monitor that detects block device changes via /proc and /sys.

    This class monitors for device hot-plug events without using lsblk (which
    can block on devices undergoing firmware wipe). Device discovery is now
    done directly via DeviceInfo.discover_devices().
    """

    def __init__(self, check_interval=0.2):
        """
        Initialize the device change monitor.

        Args:
            check_interval: How often to check for changes (seconds)
        """
        self.check_interval = check_interval
        self._lock = threading.Lock()
        self._thread = None
        self._stop_event = threading.Event()
        self._changes_detected = False
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

    def _check_for_changes(self):
        """
        Check if block devices or partitions have changed (non-blocking).

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

    def _monitor_loop(self):
        """Background thread loop that monitors for device changes"""
        while not self._stop_event.is_set():
            if self._check_for_changes():
                with self._lock:
                    self._changes_detected = True

            # Sleep for the check interval
            self._stop_event.wait(self.check_interval)

    def get_and_clear(self):
        """
        Check if changes were detected since last call.

        Returns:
            True if changes detected, False otherwise
        """
        with self._lock:
            result = self._changes_detected
            self._changes_detected = False
            return result

    def peek(self):
        """
        Check if changes were detected without clearing the flag.

        Returns:
            True if changes detected, False otherwise
        """
        with self._lock:
            return self._changes_detected
