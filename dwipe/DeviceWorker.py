"""
DeviceWorker - Background thread for non-blocking device probing

Each device gets its own worker thread that handles potentially-blocking
operations (sysfs reads, hdparm, nvme commands) without freezing the UI.
"""
import os
import threading
import queue
from enum import Enum
from types import SimpleNamespace

from .DrivePreChecker import DrivePreChecker


class ProbeState(Enum):
    """State of a probe operation"""
    PENDING = "pending"    # Not yet probed
    PROBING = "probing"    # Probe in progress
    READY = "ready"        # Value available
    FAILED = "failed"      # Probe failed/timed out
    STALE = "stale"        # Needs refresh


class DeviceWorker(threading.Thread):
    """Background worker thread for a single device.

    Handles potentially-blocking operations like:
    - Reading sysfs attributes (can block if device in D state)
    - Running hdparm/nvme commands for hardware capabilities

    Main thread reads cached state via get_state() - never blocks.
    """

    def __init__(self, device_name, checker=None):
        super().__init__(daemon=True, name=f"DeviceWorker-{device_name}")
        self.device_name = device_name
        self.checker = checker or DrivePreChecker()

        # Thread control
        self._running = True
        self._work_queue = queue.Queue()

        # Thread-safe state storage
        self._lock = threading.Lock()
        self._state = {
            # Probe states
            'hw_caps_state': ProbeState.PENDING,
            'serial_state': ProbeState.PENDING,
            'model_state': ProbeState.PENDING,

            # Cached values
            'hw_caps': {},
            'hw_nopes': {},
            'serial': '',
            'model': '',
            'vendor': '',

            # Metadata
            'last_error': None,
            'probe_count': 0,
            'is_usb': False,  # True if device is on USB bus
        }

        # Device type detection
        self._is_nvme = device_name.startswith('nv')
        self._is_sata = device_name.startswith('sd')
        self._is_hd = device_name.startswith('hd')

    def run(self):
        """Main worker loop - processes work queue."""
        while self._running:
            try:
                # Wait for work with timeout (allows clean shutdown)
                task = self._work_queue.get(timeout=0.5)
                if task == 'stop':
                    break
                elif task == 'hw_caps':
                    self._probe_hw_caps()
                elif task == 'serial':
                    self._probe_serial()
                elif task == 'model':
                    self._probe_model()
                elif task == 'refresh_all':
                    self._probe_serial()
                    self._probe_model()
            except queue.Empty:
                pass  # Normal timeout, check if still running
            except Exception as e:
                with self._lock:
                    self._state['last_error'] = str(e)

    def stop(self):
        """Signal worker to stop."""
        self._running = False
        self._work_queue.put('stop')

    def request_hw_caps(self):
        """Request hardware capabilities probe (non-blocking)."""
        with self._lock:
            if self._state['hw_caps_state'] == ProbeState.PENDING:
                self._state['hw_caps_state'] = ProbeState.PROBING
                self._work_queue.put('hw_caps')

    def request_refresh(self):
        """Request refresh of dynamic data (non-blocking)."""
        self._work_queue.put('refresh_all')

    def get_state(self):
        """Get current cached state (thread-safe, non-blocking).

        Returns:
            dict with all cached device info and probe states
        """
        with self._lock:
            return self._state.copy()

    def get_hw_caps(self):
        """Get hardware capabilities if ready.

        Returns:
            tuple: (hw_caps, hw_nopes, state, is_usb) where state is ProbeState
        """
        with self._lock:
            return (
                self._state['hw_caps'].copy(),
                self._state['hw_nopes'].copy(),
                self._state['hw_caps_state'],
                self._state['is_usb']
            )

    def _probe_hw_caps(self):
        """Probe hardware wipe capabilities (runs in worker thread)."""
        with self._lock:
            self._state['hw_caps_state'] = ProbeState.PROBING
            self._state['probe_count'] += 1
            # Detect USB (SATA-over-USB bridges can still support ATA passthrough)
            self._state['is_usb'] = self._is_usb_device()

        try:
            dev_path = f"/dev/{self.device_name}"
            if self._is_nvme:
                result = self.checker.check_nvme_drive(dev_path)
            elif self._is_sata or self._is_hd:
                result = self.checker.check_ata_drive(dev_path)
            else:
                # Unknown device type
                with self._lock:
                    self._state['hw_caps_state'] = ProbeState.READY
                return

            with self._lock:
                self._state['hw_caps'] = result.modes
                self._state['hw_nopes'] = result.issues
                self._state['hw_caps_state'] = ProbeState.READY

        except Exception as e:
            with self._lock:
                self._state['hw_caps_state'] = ProbeState.FAILED
                self._state['last_error'] = f"hw_caps: {e}"

    def _probe_serial(self):
        """Probe device serial number (runs in worker thread)."""
        with self._lock:
            self._state['serial_state'] = ProbeState.PROBING

        try:
            serial = self._read_sysfs_attr('device/serial')
            if not serial:
                # Try VPD page 80 (can be slow/blocking)
                serial = self._read_vpd_serial()

            with self._lock:
                self._state['serial'] = serial
                self._state['serial_state'] = ProbeState.READY

        except Exception as e:
            with self._lock:
                self._state['serial_state'] = ProbeState.FAILED
                self._state['last_error'] = f"serial: {e}"

    def _probe_model(self):
        """Probe device model (runs in worker thread)."""
        with self._lock:
            self._state['model_state'] = ProbeState.PROBING

        try:
            vendor = self._read_sysfs_attr('device/vendor')
            model = self._read_sysfs_attr('device/model')

            with self._lock:
                self._state['vendor'] = vendor
                self._state['model'] = model
                self._state['model_state'] = ProbeState.READY

        except Exception as e:
            with self._lock:
                self._state['model_state'] = ProbeState.FAILED
                self._state['last_error'] = f"model: {e}"

    def _read_sysfs_attr(self, attr_path):
        """Read a sysfs attribute for this device.

        Note: This CAN block if device is in D state.
        That's why it runs in the worker thread, not main thread.
        """
        path = f"/sys/class/block/{self.device_name}/{attr_path}"
        try:
            with open(path, 'r') as f:
                return f.read().strip()
        except (FileNotFoundError, IOError, OSError):
            return ''

    def _read_vpd_serial(self):
        """Read serial from VPD page 80 (SCSI).

        WARNING: This can block indefinitely on unresponsive devices.
        """
        path = f"/sys/class/block/{self.device_name}/device/vpd_pg80"
        try:
            with open(path, 'rb') as f:
                data = f.read()
                if len(data) > 4:
                    return data[4:].decode('ascii', errors='ignore').strip()
        except (FileNotFoundError, IOError, OSError):
            pass
        return ''

    def _is_usb_device(self):
        """Check if device is connected via USB bus."""
        try:
            sysfs_path = f'/sys/class/block/{self.device_name}'
            real_path = os.path.realpath(sysfs_path).lower()
            return '/usb' in real_path
        except (OSError, IOError):
            return False


class DeviceWorkerManager:
    """Manages DeviceWorker instances for all devices.

    Creates workers when devices appear, stops them when devices disappear.
    Provides unified interface for main thread to access device state.
    """

    def __init__(self, checker=None):
        self.checker = checker or DrivePreChecker()
        self._workers = {}  # device_name -> DeviceWorker
        self._lock = threading.Lock()

    def update_devices(self, device_names):
        """Update worker set based on current device list.

        Args:
            device_names: set or list of current device names (e.g., {'sda', 'nvme0n1'})
        """
        device_names = set(device_names)

        with self._lock:
            current = set(self._workers.keys())

            # Stop workers for removed devices
            for name in current - device_names:
                worker = self._workers.pop(name)
                worker.stop()

            # Create workers for new devices
            for name in device_names - current:
                # Only create workers for whole disks (sd*, nvme*n*, hd*)
                if self._is_whole_disk(name):
                    worker = DeviceWorker(name, self.checker)
                    worker.start()
                    self._workers[name] = worker

    def request_hw_caps(self, device_name):
        """Request hardware capabilities probe for a device."""
        with self._lock:
            worker = self._workers.get(device_name)
            if worker:
                worker.request_hw_caps()

    def get_hw_caps(self, device_name):
        """Get hardware capabilities for a device.

        Returns:
            tuple: (hw_caps, hw_nopes, state, is_usb) or ({}, {}, ProbeState.PENDING, False) if no worker
        """
        with self._lock:
            worker = self._workers.get(device_name)
            if worker:
                return worker.get_hw_caps()
        return ({}, {}, ProbeState.PENDING, False)

    def get_state(self, device_name):
        """Get full cached state for a device."""
        with self._lock:
            worker = self._workers.get(device_name)
            if worker:
                return worker.get_state()
        return None

    def stop_all(self):
        """Stop all workers (call on shutdown)."""
        with self._lock:
            for worker in self._workers.values():
                worker.stop()
            # Wait for all to finish
            for worker in self._workers.values():
                worker.join(timeout=2.0)
            self._workers.clear()

    @staticmethod
    def _is_whole_disk(name):
        """Check if device name is a whole disk (not a partition)."""
        # NVMe: nvme0n1 is disk, nvme0n1p1 is partition
        if name.startswith('nvme'):
            return 'p' not in name.split('n')[-1]
        # SATA/IDE: sda is disk, sda1 is partition (partitions end with digits)
        if name.startswith(('sd', 'hd')):
            return not name[-1].isdigit()
        return False
