"""
DeviceInfo class for device discovery and information management
"""
# pylint: disable=invalid-name,broad-exception-caught
# pylint: disable=line-too-long,too-many-locals,too-many-branches
# pylint: disable=too-many-return-statements,too-many-nested-blocks
# pylint: disable=too-many-statements

import os
import re
import json
import subprocess
import time
import datetime
import curses
import traceback
from fnmatch import fnmatch
from types import SimpleNamespace
from console_window import Theme

from .WipeJob import WipeJob
from .Utils import Utils
from .DrivePreChecker import DrivePreChecker
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
                               hw_nopes={},   # hw reasons cannot do hw wipe
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
        ns.hw_caps, ns.hw_nopes = {}, {}

        # 4. Perform the actual Probe
        dev_path = f"/dev/{ns.name}"
        if ns.name.startswith('nv'):
            result = self.checker.check_nvme_drive(dev_path)
        elif ns.name.startswith('sd'):
            result = self.checker.check_ata_drive(dev_path)
        # 5. Store Results
        ns.hw_caps, ns.hw_nopes = result.modes, result.issues
        return ns.hw_caps, ns.hw_nopes

    def _get_port_from_sysfs(self, device_name):
        try:
            sysfs_path = f'/sys/class/block/{device_name}'
            if not os.path.exists(sysfs_path):
                return ''

            real_path = os.path.realpath(sysfs_path).lower()

            # 1. USB - Format: USB:1-1.4
            if '/usb' in real_path:
                usb_match = re.search(r'/(\d+-\d+(?:\.\d+)*):', real_path)
                if usb_match:
                    return f"USB:{usb_match.group(1)}"

            # 2. SATA - Format: SATA:1
            elif '/ata' in real_path:
                ata_match = re.search(r'ata(\d+)', real_path)
                if ata_match:
                    return f"SATA:{ata_match.group(1)}"

            # 3. NVMe - Format: PCI:1b.0 (Stripped of 0000:00: noise)
            elif '/nvme' in real_path:
                # This regex ignores the 4-digit domain and the first 2-digit bus
                pci_match = re.search(r'0000:[0-9a-f]{2}:([0-9a-f]{2}\.[0-9a-f])', real_path)
                if pci_match:
                    return f"PCI:{pci_match.group(1)}"
                return "NVMe"

            # 4. MMC/eMMC - Format: MMC:0 or PCI:1a.0 (if PCI-attached)
            elif '/mmc' in real_path:
                # Try to extract mmc host number
                mmc_match = re.search(r'/mmc_host/mmc(\d+)', real_path)
                if mmc_match:
                    return f"MMC:{mmc_match.group(1)}"
                # Fallback: try to get PCI address if available
                pci_match = re.search(r'0000:[0-9a-f]{2}:([0-9a-f]{2}\.[0-9a-f])', real_path)
                if pci_match:
                    return f"PCI:{pci_match.group(1)}"
                return "MMC"

        except Exception as e:
            # Log exception to file for debugging
            with open('/tmp/dwipe_port_debug.log', 'a', encoding='utf-8') as f:
                f.write(f"Exception in _get_port_from_sysfs({device_name}): {e}\n")
                traceback.print_exc(file=f)
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

    def parse_lsblk(self, dflt, prev_nss=None, lsblk_output=None):
        """Parse ls_blk for all the goodies we need

        Args:
            dflt: Default state for new devices
            prev_nss: Previous device namespaces for merging
            lsblk_output: Optional lsblk JSON output string. If provided, uses this
                         instead of running lsblk command. Useful for background monitoring.
        """
        def eat_one(device):
            entry = self._make_partition_namespace(0, '', '', dflt)
            entry.name = device.get('name', '')
            maj_min = device.get('maj:min', (-1, -1))
            wds = maj_min.split(':', maxsplit=1)
            entry.major = -1
            if len(wds) > 0:
                entry.major = int(wds[0])
            entry.fstype = device.get('fstype', '')
            if entry.fstype is None:
                entry.fstype = ''
            entry.type = device.get('type', '')
            entry.label = device.get('label', '')
            if not entry.label:
                entry.label = device.get('partlabel', '')
            if entry.label is None:
                entry.label = ''
            entry.size_bytes = int(device.get('size', 0))

            # Get UUID - prefer PARTUUID for partitions, UUID for filesystems
            entry.uuid = device.get('partuuid', '') or device.get('uuid', '') or ''
            entry.serial = device.get('serial', '') or ''

            mounts = device.get('mountpoints', [])
            while len(mounts) >= 1 and mounts[0] is None:
                del mounts[0]
            entry.mounts = mounts

            # Check if we should read the marker (3-state model: dont-know, got-marker, no-marker)
            # Read marker ONCE when:
            # 1. Not mounted
            # 2. No filesystem (fstype/label empty)
            # 3. No active job
            # 4. Haven't checked yet (marker_checked=False)
            has_job = prev_nss and entry.name in prev_nss and getattr(prev_nss[entry.name], 'job', None) is not None
            has_filesystem = entry.fstype or entry.label

            # Inherit marker_checked from previous scan, or False if new/changed
            prev_had_filesystem = (prev_nss and entry.name in prev_nss and
                                   (prev_nss[entry.name].fstype or prev_nss[entry.name].label))
            filesystem_changed = prev_had_filesystem != bool(has_filesystem)

            if prev_nss and entry.name in prev_nss and not filesystem_changed:
                entry.marker_checked = prev_nss[entry.name].marker_checked

            # Read marker if haven't checked yet and safe to do so
            should_read_marker = (not mounts and not has_filesystem and not has_job and
                                  not entry.marker_checked)

            if should_read_marker:
                entry.marker_checked = True  # Mark as checked regardless of result
                marker = WipeJob.read_marker_buffer(entry.name)
                now = int(round(time.time()))
                if (marker and marker.size_bytes == entry.size_bytes
                        and marker.unixtime < now):
                    # For multi-pass wipes, scrubbed_bytes can exceed size_bytes
                    # Calculate completion percentage (capped at 100%)
                    pct = min(100, int(round((marker.scrubbed_bytes / marker.size_bytes) * 100)))
                    state = 'W' if pct >= 100 else 's'
                    dt = datetime.datetime.fromtimestamp(marker.unixtime)
                    # Add verification status prefix
                    verify_prefix = ''
                    verify_status = getattr(marker, 'verify_status', None)
                    if verify_status == 'pass':
                        verify_prefix = '✓ '
                    elif verify_status == 'fail':
                        verify_prefix = '✗ '
                    entry.marker = f'{verify_prefix}{state} {pct}% {marker.mode} {dt.strftime("%Y/%m/%d %H:%M")}'
                    entry.state = state
                    entry.dflt = state  # Set dflt so merge logic knows this partition has a marker

            return entry

        # Get lsblk output - either from parameter or by running command
        if lsblk_output:  # Non-empty string from background monitor
            # Use provided output string
            try:
                parsed_data = json.loads(lsblk_output)
            except (json.JSONDecodeError, Exception):
                # Invalid JSON - return empty dict
                return {}
        else:
            # Run the `lsblk` command and get its output in JSON format with additional columns
            # Use timeout to prevent UI freeze if lsblk hangs on problematic devices
            try:
                result = subprocess.run(['lsblk', '-J', '--bytes', '-o',
                                        'NAME,MAJ:MIN,FSTYPE,TYPE,LABEL,PARTLABEL,FSUSE%,SIZE,MOUNTPOINTS,UUID,PARTUUID,SERIAL'],
                                       stdout=subprocess.PIPE, text=True, check=False, timeout=10.0)
                parsed_data = json.loads(result.stdout)
            except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):  # pylint: disable=broad-exception-caught
                # lsblk hung, returned bad JSON, or other error - return empty dict
                # assemble_partitions will detect this and preserve previous state
                return {}
        entries = {}

        # Parse each block device and its properties
        for device in parsed_data['blockdevices']:
            parent = eat_one(device)
            entries[parent.name] = parent
            for child in device.get('children', []):
                entry = eat_one(child)
                entries[entry.name] = entry
                entry.parent = parent.name
                parent.minors.append(entry.name)
                self.disk_majors.add(entry.major)
                if entry.mounts:
                    entry.state = 'Mnt'
                    parent.state = 'Mnt'


        # Final pass: Identify disks, assign ports, and handle superfloppies
        final_entries = {}
        for name, entry in entries.items():
            final_entries[name] = entry

            # Only process top-level physical disks
            if entry.parent is None:
                # Hardware Info Gathering
                entry.model = self._get_device_vendor_model(entry.name)
                entry.port = self._get_port_from_sysfs(entry.name)

                # The Split (Superfloppy Case)
                # If it has children, the children already hold the data.
                # If it has NO children but HAS data, we create the '----' child.
                if not entry.minors and (entry.fstype or entry.label or entry.mounts):
                    v_key = f"{name}_data"
                    v_child = self._make_partition_namespace(entry.major, name, entry.size_bytes, dflt)
                    v_child.name = "----"
                    v_child.fstype = entry.fstype
                    v_child.label = entry.label
                    v_child.mounts = entry.mounts
                    v_child.parent = name

                    final_entries[v_key] = v_child
                    entry.minors.append(v_key)

                # Clean the hardware row of data-specific strings
                entry.fstype = entry.model if entry.model else 'DISK'
                entry.label = ''
                entry.mounts = []

        entries = final_entries

        if self.DB:
            print('\n\nDB: --->>> after parse_lsblk:')
            for entry in entries.values():
                print(vars(entry))

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
        if to == 'Blk' and not state_in(ns.state, list(ready_states) + ['Mnt']):
            return False
        if to == 'Unbl' and ns.state != 'Blk':
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
            if parent.state != 'Blk':
                parent.state = ns.state
        if state_in(ns.state, job_states):
            if parent:
                parent.state = 'Busy'
            for minor in minors:
                minor.state = 'Busy'
        return True

    @staticmethod
    def clear_inferred_states(nss):
        """Clear all inferred states (Busy, Mnt) so they can be re-inferred"""
        inferred_states = ('Busy', 'Mnt')
        for ns in nss.values():
            if ns.state in inferred_states:
                ns.state = ns.dflt

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
            if ns.size_bytes <= 0: # not relevant to wiping
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
            if ns.state in ('Mnt', 'Blk'):
                # Dim the orange for mounted/blocked devices
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
        elif ns.state in ('Mnt', 'Blk'):
            # Dim mounted or blocked devices
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

                if prev_ns.state == 'Blk':
                    new_ns.state = 'Blk'
                elif new_ns.state not in ('s', 'W'):
                    new_ns.state = new_ns.dflt
                    # Don't copy forward percentage states (like "v96%") - only persistent states
                    if prev_ns.state not in ('s', 'W', 'Busy', 'Unbl') and not prev_ns.state.endswith('%'):
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

    def assemble_partitions(self, prev_nss=None, lsblk_output=None):
        """Assemble and filter partitions for display

        Args:
            prev_nss: Previous device namespaces for merging
            lsblk_output: Optional lsblk JSON output string from LsblkMonitor
        """
        nss = self.parse_lsblk(dflt='^' if prev_nss else '-', prev_nss=prev_nss,
                               lsblk_output=lsblk_output)

        # If parse_lsblk failed (returned empty) and we have previous data, keep previous state
        if not nss and prev_nss:
            # lsblk scan failed or returned no devices - preserve previous state
            # This prevents losing devices when lsblk temporarily fails
            # But clear temporary status messages from completed jobs
            for ns in prev_nss.values():
                if not ns.job and ns.mounts:
                    # Job finished - clear temporary status messages like "Verified: zeroed"
                    ns.mounts = [m for m in ns.mounts if not m.startswith(('Verified:', 'Stopped'))]
            return prev_nss  # Return early - don't reprocess

        nss = self.get_disk_partitions(nss)

        nss = self.merge_dev_infos(nss, prev_nss)

        # Apply persistent blocked states
        if self.persistent_state:
            for ns in nss.values():
                # Update last_seen timestamp
                self.persistent_state.update_device_seen(ns)
                # Apply persistent block state
                if self.persistent_state.get_device_locked(ns):
                    ns.state = 'Blk'

        # Clear inferred states so they can be re-computed based on current job status
        self.clear_inferred_states(nss)
        self.set_all_states(nss)  # set inferred states

        self.compute_field_widths(nss)

        if self.DB:
            print('\n\nDB: --->>> after assemble_partitions():')
            for name, ns in nss.items():
                print(f'DB: {name}: {vars(ns)}')
        self.partitions = nss
        return nss
