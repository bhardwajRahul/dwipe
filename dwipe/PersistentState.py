"""
PersistentState class for saving user preferences and device states
"""
# pylint: disable=invalid-name,broad-exception-caught,line-too-long
import json
import time
from pathlib import Path
from .Utils import Utils


class PersistentState:
    """Manages persistent state for dwipe preferences and device blocks"""

    def __init__(self, config_path=None):
        """Initialize persistent state

        Args:
            config_path: Path to config file (default: ~/.config/dwipe/state.json)
        """
        if config_path is None:
            config_dir = Utils.get_config_dir()
            config_path = config_dir / 'state.json'

        self.config_path = Path(config_path)
        self.state = {
            'theme': 'default',
            'wipe_mode': 'Zero',  # 'Rand' or 'Zero' or +V
            'passes': 1,  # 1, 2, or 4 wipe pass
            'confirmation': 'YES',  # 'Y', 'y', 'YES', 'yes', 'device'
            'verify_pct': 2,  # 0, 2, 5, 10, 25, 50, 100
            'dense': False,  # True = compact view, False = blank lines between disks
            'slowdown_stop': 16,
            'stall_timeout': 60,
            'port_serial': False,
            'devices': {}  # device_id -> {blocked, last_seen, last_name, size_bytes}
        }
        self.dirty = False
        self.max_devices = 400

        self.load()

    def save_updated_opts(self, opts):
        """Save updated option variables from opts to state"""
        for key in self.state:
            if key == 'devices':
                continue  # Skip devices dict
            if hasattr(opts, key):
                value = getattr(opts, key)
                if self.state[key] != value:
                    self.state[key] = value
                    self.dirty = True

    def restore_updated_opts(self, opts):
        """Restore option variables from state to opts"""
        for key in self.state:
            if hasattr(opts, key):
                setattr(opts, key, self.state[key])

    def load(self):
        """Load state from disk"""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    self.state.update(loaded)
            except (json.JSONDecodeError, IOError) as e:
                print(f'Warning: Could not load state from {self.config_path}: {e}')

    def save(self):
        """Save state to disk if dirty"""
        if not self.dirty:
            return

        try:
            # Clean up old devices before saving
            self._cleanup_old_devices()

            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2)
            # Fix ownership if running with sudo
            Utils.fix_file_ownership(self.config_path)
            self.dirty = False
        except IOError as e:
            print(f'Warning: Could not save state to {self.config_path}: {e}')

    def _cleanup_old_devices(self):
        """Keep only the most recent max_devices entries"""
        devices = self.state['devices']
        if len(devices) <= self.max_devices:
            return

        # Sort by last_seen timestamp, keep most recent
        sorted_devices = sorted(
            devices.items(),
            key=lambda x: x[1].get('last_seen', 0),
            reverse=True
        )

        # Keep only max_devices most recent
        self.state['devices'] = dict(sorted_devices[:self.max_devices])

    @staticmethod
    def make_device_id(partition):
        """Create a stable device identifier

        Priority:
        1. partition.uuid (PARTUUID for partitions, UUID for filesystems)
        2. partition.serial (for whole disks)
        3. Fallback: hash of name+size+label+fstype

        Args:
            partition: SimpleNamespace with device info

        Returns:
            str: Stable device identifier
        """
        # Try UUID first (PARTUUID or filesystem UUID)
        if hasattr(partition, 'uuid') and partition.uuid:
            return f'uuid:{partition.uuid}'

        # Try serial number (for whole disks)
        if hasattr(partition, 'serial') and partition.serial:
            return f'serial:{partition.serial}'

        # Fallback: create stable ID from device characteristics
        # This will break if device is repartitioned/reformatted, but that's acceptable
        parts = [
            partition.name,
            str(partition.size_bytes),
            partition.label if hasattr(partition, 'label') else '',
            partition.fstype if hasattr(partition, 'fstype') else ''
        ]
        fallback_id = ':'.join(parts)
        return f'fallback:{fallback_id}'

    def get_device_locked(self, partition):
        """Check if a device is blocked (backward compatible with 'locked')

        Args:
            partition: SimpleNamespace with device info

        Returns:
            bool: True if device is blocked
        """
        device_id = self.make_device_id(partition)
        device_state = self.state['devices'].get(device_id, {})

        # Check new 'blocked' field first, fall back to old 'locked' field for backward compatibility
        if 'blocked' in device_state:
            return device_state['blocked']
        return device_state.get('locked', False)

    def set_device_locked(self, partition, locked):
        """Set device block state

        Args:
            partition: SimpleNamespace with device info
            locked: bool, True to block device (parameter name kept for API compatibility)
        """
        device_id = self.make_device_id(partition)
        now = int(time.time())

        if device_id not in self.state['devices']:
            self.state['devices'][device_id] = {}

        device_state = self.state['devices'][device_id]
        device_state['blocked'] = locked  # Only save 'blocked', not 'locked'
        # Remove old 'locked' field if it exists (gradual migration)
        device_state.pop('locked', None)
        device_state['last_seen'] = now
        device_state['last_name'] = partition.name
        device_state['size_bytes'] = partition.size_bytes

        self.dirty = True

    def update_device_seen(self, partition):
        """Update last_seen timestamp for a device

        Args:
            partition: SimpleNamespace with device info
        """
        device_id = self.make_device_id(partition)
        now = int(time.time())

        if device_id in self.state['devices']:
            self.state['devices'][device_id]['last_seen'] = now
            self.state['devices'][device_id]['last_name'] = partition.name
            self.dirty = True


    def sync(self):
        """Save state if dirty (called each loop)"""
        self.save()
