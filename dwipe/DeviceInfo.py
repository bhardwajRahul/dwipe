"""
DeviceInfo class for device discovery and information management
"""
import os
import re
import json
import subprocess
import time
import datetime
import curses
from fnmatch import fnmatch
from types import SimpleNamespace
from .WipeJob import WipeJob
from .Utils import Utils
from .DrivePreChecker import DrivePreChecker

from console_window import Theme
# from .PersistentState import PersistentState


class DeviceInfo:
    """Class to dig out the info we want from the system."""
    disk_majors = set()  # major devices that are disks

    def __init__(self, opts, persistent_state=None):
        self.opts = opts
        self.checker = DrivePreChecker()
        self.DB = opts.debug
        self.wids = None
        self.head_str = None
        self.partitions = None
        self.persistent_state = persistent_state
        self._lsblk_columns = None
        self._do_discover_lsblk_columns = True

    def _get_supported_columns(self):
        """Probe lsblk once to see which columns it supports."""
        # Absolute essentials
        base = ['name', 'maj:min', 'fstype', 'type', 'label', 'partlabel', 
                'size', 'mountpoints', 'uuid', 'partuuid', 'serial', 'model']
        
        # Modern columns to test
        extras = ['tran', 'id-path']
        supported = list(base)
        
        for col in extras:
            try:
                # Test column support with a dummy call
                res = subprocess.run(['lsblk', '-o', col], 
                         check=False, capture_output=True, text=True)
                if res.returncode == 0:
                    supported.append(col)
            except Exception:
                continue
                
        self._lsblk_columns = ",".join(supported)
        self._do_discover_lsblk_columns = False
        if self.DB:
            print(f"DB: lsblk columns discovered: {self._lsblk_columns}")
        return self._lsblk_columns

    @staticmethod
    def _make_partition_namespace(major, name, size_bytes, dflt):
        return SimpleNamespace(name=name,       # /proc/partitions
                               major=major,       # /proc/partitions
                               parent=None,     # a partition
                               state=dflt,         # run-time state
                               dflt=dflt,         # default run-time state
                               label='',       # blkid
                               fstype='',      # blkid
                               type='',        # device type (disk, part)
                               model='',       # /sys/class/block/{name}/device/vendor|model
                               size_bytes=size_bytes,  # /sys/block/{name}/...
                               marker='',      #  persistent status
                               marker_checked=False,  # True if we've read the marker once
                               mounts=[],        # /proc/mounts
                               minors=[],
                               job=None,         # if zap running
                               uuid='',        # filesystem UUID or PARTUUID
                               serial='',      # disk serial number (for whole disks)
                               port='',        # port (for whole disks)
                               hw_caps={},      # hw_wipe capabilities (for whole disks)
                               hw_nopes=[],   # hw reasons cannot do hw wipe
                               )

    def get_hw_capabilities(self, ns):
        """
        Populates and returns hardware wipe capabilities for a disk.
        Returns cached data if already present.
        """
        # 1. Check if we already have cached results
        if hasattr(ns, 'hw_caps') and (ns.hw_caps or ns.hw_nopes):
            return ns.hw_caps, ns.hw_nopes

        # Initialize defaults
        ns.hw_caps = {}
        ns.hw_nopes = []

#       # 2. Basic Pre-Flight: Is it a disk? Is it busy?
#       if ns.type != 'disk':
#           ns.hw_nopes.append("Not a physical disk")
#           return ns.hw_caps, ns.hw_nopes

#       if ns.state == 'Mnt' or ns.mounts:
#           ns.hw_nopes.append("Drive has active mounts")
#           return ns.hw_caps, ns.hw_nopes

#       # 3. Port Check: USB Safety
#       # We use the port string we worked so hard to get earlier
#       if ns.port and "USB" in ns.port:
#           ns.hw_nopes.append("USB bridges are unsafe for FW wipes")
#           return ns.hw_caps, ns.hw_nopes

        # 4. Perform the actual Probe
        dev_path = f"/dev/{ns.name}"
        
        if "nvme" in ns.name:
            result = self.checker.check_nvme_drive(dev_path)
        else:
            result = self.checker.check_ata_drive(dev_path)

        # 5. Store Results
        ns.hw_caps = result.modes
        ns.hw_nopes = result.issues

        return ns.hw_caps, ns.hw_nopes


    def _get_port_from_sysfs(self, device_name):
        """ TBD """
        try:
            sysfs_path = f'/sys/class/block/{device_name}'
            real_path = os.path.realpath(sysfs_path).lower()

            # 1. USB - Capture the specific bus/port (e.g., 2-3)
            if '/usb' in real_path:
                # Matches the '2-3' in .../usb2/2-3/2-3:1.0/...
                usb_match = re.search(r'/(\d+-\d+(?:\.\d+)*):', real_path)
                return f"USB:{usb_match.group(1)}" if usb_match else "USB"

            # 2. SATA - Capture the host number (e.g., SATA:1)
            elif '/ata' in real_path:
                # Matches the '1' in .../ata1/host1/...
                ata_match = re.search(r'ata(\d+)', real_path)
                return f"SATA:{ata_match.group(1)}" if ata_match else "SATA"

            # 3. NVMe - Capture the PCI slot
            elif '/nvme' in real_path:
                pci_match = re.search(r'0000:[0-9a-f]{2}:([0-9a-f]{2}\.[0-9a-f])', real_path)
                return f"PCI:{pci_match.group(1)}" if pci_match else "NVMe"

        except Exception:
            pass
        return ''

    def _get_port_info(self, device):
        """
        Uses lsblk for the protocol (TRAN), but relies on sysfs 
        to get the specific port/socket number.
        """
        name = device.get('name') or ''
        # Get the detailed string (e.g., "SATA:1" or "USB:2-3")
        sysfs_port = self._get_port_from_sysfs(name)
        
        if sysfs_port:
            return sysfs_port

        # Fallback: if sysfs failed but lsblk has transport info
        tran = (device.get('tran') or '').upper()
        if tran:
            return tran # Returns "USB", "SATA", "NVME" etc.
            
        return ''

    @staticmethod
    def _get_device_vendor_model(device_name):
        """Gets the vendor and model for a given device from the /sys/class/block directory.
        - Args: - device_name: The device name, such as 'sda', 'sdb', etc.
        - Returns: A string containing the vendor and model information.
        """
        def get_str(device_name, suffix):
            try:
                rv = ''
                fullpath = f'/sys/class/block/{device_name}/device/{suffix}'
                with open(fullpath, 'r', encoding='utf-8') as f:  # Read information
                    rv = f.read().strip()
            except (FileNotFoundError, Exception):
                pass
            return rv

        rv = f'{get_str(device_name, "model")}'
        return rv.strip()


    def _format_marker_string(self, marker, entry_size):
        """Converts raw marker data into the UI string (e.g., '✓ W 100% Zero 2026/01/05')"""
        now = int(round(time.time()))
        if not (marker and marker.size_bytes == entry_size and marker.unixtime < now):
            return None

        # Calculate completion percentage
        pct = min(100, int(round((marker.scrubbed_bytes / marker.size_bytes) * 100)))
        state = 'W' if pct >= 100 else 's'
        
        # Format Date
        dt = datetime.datetime.fromtimestamp(marker.unixtime)
        date_str = dt.strftime("%Y/%m/%d %H:%M")
        
        # Verification Prefix
        verify_prefix = ''
        verify_status = getattr(marker, 'verify_status', None)
        if verify_status == 'pass':
            verify_prefix = '✓ '
        elif verify_status == 'fail':
            verify_prefix = '✗ '

        return f'{verify_prefix}{state} {pct}% {marker.mode} {date_str}'

    def parse_lsblk(self, dflt, prev_nss=None):
        """ PARSE lsblk output """

        cols = self._lsblk_columns if not self._do_discover_lsblk_columns else self._get_supported_columns()

        try:
            result = subprocess.run(['lsblk', '-J', '--bytes', '-o', cols],
                                   capture_output=True, text=True, timeout=10.0, check=True)
            parsed_data = json.loads(result.stdout)
        except Exception:
            return prev_nss or {}

        # The flat dictionary that the UI actually uses
        entries = {}

        def process_node(device, parent_name=None):
            name = device.get('name', '')
            if not name: 
                return None
            
            # 1. Create the basic namespace entry
            entry = self._make_partition_namespace(0, name, int(device.get('size', 0)), dflt)
            
            # 2. Fill standard fields
            maj_min = (device.get('maj:min') or '0:0').split(':')
            entry.major = int(maj_min[0])
            entry.fstype = device.get('fstype') or ''
            entry.type = device.get('type') or ''
            entry.label = device.get('label') or device.get('partlabel') or ''
            entry.uuid = device.get('partuuid') or device.get('uuid') or ''
            entry.serial = device.get('serial') or ''
            entry.mounts = [m for m in device.get('mountpoints', []) if m]
            entry.parent = parent_name
            entry.minors = [] # Initialize empty for recursion

            # 3. Hardware Discovery (Only for top-level disks)
            if entry.type == 'disk':
                # Guard against None values from JSON
                raw_model = device.get('model') or ''
                entry.model = raw_model.strip()
                # If model is still empty, try the fallback
                if not entry.model:
                    entry.model = self._get_device_vendor_model(entry.name)
                entry.port = self._get_port_info(device)
            
            # 4. Marker Logic
            has_job = prev_nss and name in prev_nss and getattr(prev_nss[name], 'job', None)
            if not entry.mounts and not (entry.fstype or entry.label) and not has_job:
                if prev_nss and name in prev_nss:
                    entry.marker_checked = prev_nss[name].marker_checked
                
                if not entry.marker_checked:
                    entry.marker_checked = True
                    marker_data = WipeJob.read_marker_buffer(entry.name)
                    if marker_data:
                        formatted = self._format_marker_string(marker_data, entry.size_bytes)
                        if formatted:
                            entry.marker = formatted
                            entry.state = formatted.split()[0] if ' ' in formatted else 'W'
                            entry.dflt = entry.state

            # 5. Store THIS entry in the flat dict BEFORE processing children
            entries[name] = entry

            # 6. Recurse through children (Partitions/LVM)
            children = device.get('children', [])
            for child in children:
                child_entry = process_node(child, name)
                if child_entry:
                    entry.minors.append(child_entry.name)
                    # Propagate "Mnt" state upward to the disk
                    if child_entry.state == 'Mnt':
                        entry.state = 'Mnt'

            # 7. Superfloppy Logic (Post-child check)
            if entry.type == 'disk' and not entry.minors and (entry.fstype or entry.label or entry.mounts):
                v_name = f"{name}_data"
                v_child = self._make_partition_namespace(entry.major, "----", entry.size_bytes, dflt)
                v_child.parent = name
                v_child.fstype, v_child.label, v_child.mounts = entry.fstype, entry.label, entry.mounts
                if entry.mounts: v_child.state = 'Mnt'
                
                entries[v_name] = v_child
                entry.minors.append(v_name)
                entry.fstype = entry.model or 'DISK'
                entry.label, entry.mounts = '', []

            return entry

        # Trigger the recursion
        for dev in parsed_data.get('blockdevices', []):
            process_node(dev)

        return entries

    @staticmethod
    def set_one_state(nss, ns, to=None, test_to=None):
        """Optionally, update a state, and always set inferred states"""
        ready_states = ('s', 'W', '-', '^')
        job_states = ('*%', 'STOP')
        inferred_states = ('Busy', 'Mnt',)

        def state_in(to, states):
            return to in states or fnmatch(to, states[0])

        to = test_to if test_to else to

        parent, minors = None, []
        if ns.parent:
            parent = nss.get(ns.parent)
        for minor in ns.minors:
            minor_ns = nss.get(minor, None)
            if minor_ns:
                minors.append(minor_ns)

        if to == 'STOP' and not state_in(ns.state, job_states):
            return False
        if to == 'Lock' and not state_in(ns.state, list(ready_states) + ['Mnt']):
            return False
        if to == 'Unlk' and ns.state != 'Lock':
            return False

        if to and fnmatch(to, '*%'):
            if not state_in(ns.state, ready_states):
                return False
            for minor in minors:
                if not state_in(minor.state, ready_states):
                    return False
        elif to in ('s', 'W') and not state_in(ns.state, job_states):
            return False
        if test_to:
            return True

        if to is not None:
            ns.state = to

        # Here we set inferences that block starting jobs
        #  -- clearing these states will be done on the device refresh
        if parent and state_in(ns.state, inferred_states):
            if parent.state != 'Lock':
                parent.state = ns.state
        if state_in(ns.state, job_states):
            if parent:
                parent.state = 'Busy'
            for minor in minors:
                minor.state = 'Busy'
        return True

    @staticmethod
    def set_all_states(nss):
        """Set every state per linkage inferences"""
        for ns in nss.values():
            DeviceInfo.set_one_state(nss, ns)

    def get_disk_partitions(self, nss):
        """Filter to only wipeable physical storage using positive criteria.

        Keeps devices that:
        - Are type 'disk' or 'part' (from lsblk)
        - Are writable (not read-only)
        - Are real block devices (not virtual)

        This automatically excludes:
        - Virtual devices (zram, loop, dm-*, etc.)
        - Read-only devices (CD-ROMs, eMMC boot partitions)
        - Special partitions (boot loaders)
        """
        ok_nss = {}
        for name, ns in nss.items():
            # Must be disk or partition type
            if ns.type not in ('disk', 'part'):
                continue

            # Must be writable (excludes CD-ROMs, eMMC boot partitions, etc.)
            ro_path = f'/sys/class/block/{name}/ro'
            try:
                with open(ro_path, 'r', encoding='utf-8') as f:
                    if f.read().strip() != '0':
                        continue  # Skip read-only devices
            except (FileNotFoundError, Exception):
                # If we can't read ro flag, skip this device to be safe
                continue

            # Exclude common virtual device prefixes as a safety net
            # (most should already be filtered by ro check or missing sysfs)
            virtual_prefixes = ('zram', 'loop', 'dm-', 'ram')
            if any(name.startswith(prefix) for prefix in virtual_prefixes):
                continue

            # Include this device
            ok_nss[name] = ns

        return ok_nss

    def compute_field_widths(self, nss):
        """Compute field widths for display formatting"""
        wids = self.wids = SimpleNamespace(state=5, name=4, human=7, fstype=4, label=5)
        for ns in nss.values():
            wids.state = max(wids.state, len(ns.state))
            wids.name = max(wids.name, len(ns.name) + 2)
            if ns.label is None:
                pass
            wids.label = max(wids.label, len(ns.label))
            wids.fstype = max(wids.fstype, len(ns.fstype))
        self.head_str = self.get_head_str()
        if self.DB:
            print('\n\nDB: --->>> after compute_field_widths():')
            print(f'self.wids={vars(wids)}')

    def get_head_str(self):
        """Generate header string for device list"""
        sep = '  '
        wids = self.wids
        emit = f'{"STATE":_^{wids.state}}'
        emit += f'{sep}{"NAME":_^{wids.name}}'
        emit += f'{sep}{"SIZE":_^{wids.human}}'
        emit += f'{sep}{"TYPE":_^{wids.fstype}}'
        emit += f'{sep}{"LABEL":_^{wids.label}}'
        emit += f'{sep}MOUNTS/STATUS'
        return emit

    def get_pick_range(self):
        """Calculate column range for pick highlighting (NAME through LABEL fields)"""
        sep = '  '
        wids = self.wids
        # Start just before NAME field
        start_col = wids.state + len(sep)
        # End after LABEL field (always spans through LABEL for disks)
        end_col = wids.state + len(sep) + wids.name + len(sep) + wids.human + len(sep) + wids.fstype # + len(sep) + wids.label
        return [start_col, end_col]

    def part_str(self, partition, is_last_child=False):
        """Convert partition to human value.

        Args:
            partition: Partition namespace
            is_last_child: If True and partition has parent, use └ instead of │

        Returns:
            tuple: (text, attr) where attr is curses attribute or None
        """
        def print_str_or_dash(name, width, empty='-'):
            if not name.strip():
                name = empty
            return f'{name:^{width}}'

        sep = '  '
        ns = partition  # shorthand
        wids = self.wids
        emit = f'{ns.state:^{wids.state}}'

        # Determine tree prefix character
        if ns.parent is None:
            # Physical disk: box symbol
            prefix = '■ '
        elif is_last_child:
            # Last partition of disk: rounded corner
            prefix = '└ '
        else:
            # Regular partition: vertical line
            prefix = '│ '

        name_str = prefix + ns.name

        emit += f'{sep}{name_str:<{wids.name}}'
        emit += f'{sep}{Utils.human(ns.size_bytes):>{wids.human}}'
        emit += sep + print_str_or_dash(ns.fstype, wids.fstype)
        if ns.parent is None:
            # Physical disk - always show thick line in LABEL field (disks don't have labels)
            emit += sep + '━' * wids.label
            if ns.mounts:
                # Disk has mounts - show them
                emit += f'{sep}{",".join(ns.mounts)}'
            elif ns.marker and ns.marker.strip():
                # Disk has wipe status - show it
                emit += f'{sep}{ns.marker}'
            else:
                # No status - show heavy line divider (start 1 char left to fill gap)
                emit += '━' + '━' * 30
        else:
            # Partition: show label and mount/status info
            emit += sep + print_str_or_dash(ns.label, wids.label)
            if ns.mounts:
                emit += f'{sep}{",".join(ns.mounts)}'
            else:
                emit += f'{sep}{ns.marker}'

        # Determine color attribute based on state
        attr = None
        # Check for newly inserted flag first (hot-swapped devices should always show orange)
        if getattr(ns, 'newly_inserted', False):
            # Newly inserted device - orange/bright
            if ns.state in ('Mnt', 'Lock'):
                # Dim the orange for mounted/locked devices
                attr = curses.color_pair(Theme.HOTSWAP) | curses.A_DIM
            else:
                attr = curses.color_pair(Theme.HOTSWAP) | curses.A_BOLD
        elif ns.state == 's':
            # Yellow/warning color for stopped/partial wipes (with bold for visibility)
            attr = curses.color_pair(Theme.WARNING) | curses.A_BOLD
        elif ns.state == 'W' and getattr(ns, 'wiped_this_session', False):
            # Green/success color for completed wipes (done in THIS session only) - bold and bright
            attr = curses.color_pair(Theme.SUCCESS) | curses.A_BOLD
        elif ns.state == 'W':
            # Green/success color for completed wipes before this session
            attr = curses.color_pair(Theme.OLD_SUCCESS) | curses.A_BOLD
        elif ns.state.endswith('%') and ns.state not in ('0%', '100%'):
            # Active wipe in progress - bright cyan/blue with bold
            attr = curses.color_pair(Theme.INFO) | curses.A_BOLD
        elif ns.state == '^':
            # Newly inserted device (hot-swapped) - orange/bright
            attr = curses.color_pair(Theme.HOTSWAP) | curses.A_BOLD
        elif ns.state in ('Mnt', 'Lock'):
            # Dim mounted or locked devices
            attr = curses.A_DIM

        # Override with red/danger color if verify failed
        if hasattr(ns, 'verify_failed_msg') and ns.verify_failed_msg:
            attr = curses.color_pair(Theme.DANGER) | curses.A_BOLD

        return emit, attr

    def merge_dev_infos(self, nss, prev_nss=None):
        """Merge old DevInfos into new DevInfos"""
        if not prev_nss:
            return nss

        # Track which devices were physically present in last scan
        prev_physical = set()
        for name, prev_ns in prev_nss.items():
            # Only count as "physically present" if not carried forward due to job
            if not (hasattr(prev_ns, 'was_unplugged') and prev_ns.was_unplugged):
                prev_physical.add(name)

        for name, prev_ns in prev_nss.items():
            # merge old jobs forward
            new_ns = nss.get(name, None)
            if new_ns:
                if prev_ns.job:
                    new_ns.job = prev_ns.job
                # Inheritance: Carry forward capabilities 
                # ONLY if the drive hasn't changed state significantly
                if hasattr(prev_ns, 'hw_caps'):
                    new_ns.hw_caps = prev_ns.hw_caps
                    new_ns.hw_nopes = getattr(prev_ns, 'hw_nopes', [])

                # Note: Do NOT preserve port - use fresh value from current scan
                new_ns.dflt = prev_ns.dflt
                # Preserve the "wiped this session" flag
                if hasattr(prev_ns, 'wiped_this_session'):
                    new_ns.wiped_this_session = prev_ns.wiped_this_session
                # Preserve marker and marker_checked (already inherited in parse_lsblk)
                # Only preserve marker string if we haven't just read a new one
                if hasattr(prev_ns, 'marker') and not new_ns.marker:
                    new_ns.marker = prev_ns.marker

                # Preserve verify failure message ONLY for unmarked disks
                # Clear if: filesystem appeared OR partition now has a marker
                if hasattr(prev_ns, 'verify_failed_msg'):
                    # Check if partition now has marker (dflt is 'W' or 's', not '-')
                    has_marker = new_ns.dflt in ('W', 's')

                    # For whole disks (no parent): check if any child partition has filesystem
                    # For partitions: check if this partition has filesystem
                    has_filesystem = False
                    if not new_ns.parent:
                        # Whole disk - check if any child has fstype or label
                        for _, child_ns in nss.items():
                            if child_ns.parent == name and (child_ns.fstype or child_ns.label):
                                has_filesystem = True
                                break
                    else:
                        # Partition - check if it has fstype or label
                        has_filesystem = bool(new_ns.fstype or new_ns.label)

                    if has_filesystem or has_marker:
                        # Filesystem appeared or now has marker - clear the error
                        # (verify_failed_msg is only for unmarked disks)
                        if hasattr(new_ns, 'verify_failed_msg'):
                            delattr(new_ns, 'verify_failed_msg')
                    else:
                        # Still unmarked with no filesystem - persist the error
                        new_ns.verify_failed_msg = prev_ns.verify_failed_msg
                        new_ns.mounts = [prev_ns.verify_failed_msg]

                if prev_ns.state == 'Lock':
                    new_ns.state = 'Lock'
                elif new_ns.state not in ('s', 'W'):
                    new_ns.state = new_ns.dflt
                    # Don't copy forward percentage states (like "v96%") - only persistent states
                    if prev_ns.state not in ('s', 'W', 'Busy', 'Unlk') and not prev_ns.state.endswith('%'):
                        new_ns.state = prev_ns.state  # re-infer these
            elif prev_ns.job:
                # unplugged device with job..
                prev_ns.was_unplugged = True  # Mark as unplugged
                nss[name] = prev_ns  # carry forward
                prev_ns.job.do_abort = True

        # Mark newly inserted devices (not present in previous physical scan)
        for name, new_ns in nss.items():
            if name not in prev_physical and new_ns.state not in ('s', 'W'):
                new_ns.state = '^'
                new_ns.newly_inserted = True  # Mark for orange color even if locked/mounted
        return nss

    def assemble_partitions(self, prev_nss=None):
        """Assemble and filter partitions for display"""
        nss = self.parse_lsblk(dflt='^' if prev_nss else '-', prev_nss=prev_nss)

        nss = self.get_disk_partitions(nss)

        nss = self.merge_dev_infos(nss, prev_nss)

        # Apply persistent locked states
        if self.persistent_state:
            for ns in nss.values():
                # Update last_seen timestamp
                self.persistent_state.update_device_seen(ns)
                # Apply persistent lock state
                if self.persistent_state.get_device_locked(ns):
                    ns.state = 'Lock'

        self.set_all_states(nss)  # set inferred states

        self.compute_field_widths(nss)

        if self.DB:
            print('\n\nDB: --->>> after assemble_partitions():')
            for name, ns in nss.items():
                print(f'DB: {name}: {vars(ns)}')
        self.partitions = nss
        return nss

def main():
    """
    Standalone test runner for DeviceInfo.
    Run with: python3 DeviceInfo.py
    """

    # 1. Mock the options object expected by DeviceInfo
    mock_opts = SimpleNamespace(
        debug=True,
    )

    # 2. Initialize the class
    print("--- Initializing DeviceInfo Probe ---")
    discovery = DeviceInfo(mock_opts)

    # 3. Perform the scan
    # We pass '-' as the default state
    start_time = time.time()
    nss = discovery.assemble_partitions(prev_nss=None)
    elapsed = time.time() - start_time

    # 4. Display Results
    print(f"\nScan completed in {elapsed:.3f}s")
    print(f"{'NAME':<12} | {'TYPE':<6} | {'PORT':<12} | {'STATE':<5} | {'LABEL/MODEL'}")
    print("-" * 70)

    # We sort keys to keep parents and children somewhat near each other
    for name in sorted(nss.keys()):
        ns = nss[name]
        
        # Format the display line
        indent = "  └─ " if ns.parent else ""
        label_or_model = ns.label if ns.parent else ns.model
        
        print(f"{indent + ns.name:<12} | {ns.type:<6} | {getattr(ns, 'port', ''):<12} | {ns.state:<5} | {label_or_model}")

    # 5. Optional: Detailed JSON dump of a specific device for debugging fields
    if nss:
        first_device = list(nss.keys())[0]
        print(f"\n--- Detailed Data for {first_device} ---")
        # Convert SimpleNamespace to dict for JSON printing
        sample_data = vars(nss[first_device])
        # We exclude the 'job' object as it's not serializable
        sample_data = {k: v for k, v in sample_data.items() if k != 'job'}
        print(json.dumps(sample_data, indent=4))

if __name__ == "__main__":
    # Ensure dependencies are imported for the standalone test
    main()