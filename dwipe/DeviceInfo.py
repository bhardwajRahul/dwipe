"""
DeviceInfo class for device discovery and information management
"""
import json
import subprocess
import time
import datetime
import curses
from fnmatch import fnmatch
from types import SimpleNamespace

from .WipeJob import WipeJob
from .Utils import Utils
from .ConsoleWindow import COLOR_WARNING, COLOR_INFO, COLOR_SUCCESS, COLOR_HOTSWAP, COLOR_DANGER
from .PersistentState import PersistentState


class DeviceInfo:
    """Class to dig out the info we want from the system."""
    disk_majors = set()  # major devices that are disks

    def __init__(self, opts, persistent_state=None):
        self.opts = opts
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
                               model='',       # /sys/class/block/{name}/device/vendor|model
                               size_bytes=size_bytes,  # /sys/block/{name}/...
                               marker='',      #  persistent status
                               marker_checked=False,  # True if we've read the marker once
                               mounts=[],        # /proc/mounts
                               minors=[],
                               job=None,         # if zap running
                               uuid='',        # filesystem UUID or PARTUUID
                               serial='',      # disk serial number (for whole disks)
                               )

    @staticmethod
    def get_device_vendor_model(device_name):
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

    def parse_lsblk(self, dflt, prev_nss=None):
        """Parse ls_blk for all the goodies we need"""
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

        # Run the `lsblk` command and get its output in JSON format with additional columns
        # Use timeout to prevent UI freeze if lsblk hangs on problematic devices
        try:
            result = subprocess.run(['lsblk', '-J', '--bytes', '-o',
                                    'NAME,MAJ:MIN,FSTYPE,TYPE,LABEL,PARTLABEL,FSUSE%,SIZE,MOUNTPOINTS,UUID,PARTUUID,SERIAL'],
                                   stdout=subprocess.PIPE, text=True, check=False, timeout=10.0)
            parsed_data = json.loads(result.stdout)
        except subprocess.TimeoutExpired:
            # lsblk hung - return empty dict to use previous device state
            return {}
        entries = {}

        # Parse each block device and its properties
        for device in parsed_data['blockdevices']:
            parent = eat_one(device)
            parent.fstype = self.get_device_vendor_model(parent.name)
            entries[parent.name] = parent
            for child in device.get('children', []):
                entry = eat_one(child)
                entries[entry.name] = entry
                entry.parent = parent.name
                parent.minors.append(entry.name)
                if not parent.fstype:
                    parent.fstype = 'DISK'
                self.disk_majors.add(entry.major)
                if entry.mounts:
                    entry.state = 'Mnt'
                    parent.state = 'Mnt'

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
        """Filter out virtual/pseudo devices (zram, loop, etc.) while keeping physical drives.
        Keeps: sd*, nvme*, vd*, hd*, mmcblk* (physical and USB drives)
        Excludes: zram*, loop*, dm-*, sr*, ram*
        """
        # Virtual/pseudo device prefixes to exclude
        exclude_prefixes = ('zram', 'loop', 'dm-', 'sr', 'ram')

        ok_nss = {}
        for name, ns in nss.items():
            # Must be disk or partition type
            if ns.type not in ('disk', 'part'):
                continue

            # Exclude virtual/pseudo devices by name prefix
            if any(name.startswith(prefix) for prefix in exclude_prefixes):
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
                attr = curses.color_pair(COLOR_HOTSWAP) | curses.A_DIM
            else:
                attr = curses.color_pair(COLOR_HOTSWAP) | curses.A_BOLD
        elif ns.state == 's':
            # Yellow/warning color for stopped/partial wipes (with bold for visibility)
            attr = curses.color_pair(COLOR_WARNING) | curses.A_BOLD
        elif ns.state == 'W' and getattr(ns, 'wiped_this_session', False):
            # Green/success color for completed wipes (done in THIS session only) - bold and bright
            attr = curses.color_pair(COLOR_SUCCESS) | curses.A_BOLD
        elif ns.state.endswith('%') and ns.state not in ('0%', '100%'):
            # Active wipe in progress - bright cyan/blue with bold
            attr = curses.color_pair(COLOR_INFO) | curses.A_BOLD
        elif ns.state == '^':
            # Newly inserted device (hot-swapped) - orange/bright
            attr = curses.color_pair(COLOR_HOTSWAP) | curses.A_BOLD
        elif ns.state in ('Mnt', 'Lock'):
            # Dim mounted or locked devices
            attr = curses.A_DIM

        # Override with red/danger color if verify failed
        if hasattr(ns, 'verify_failed_msg') and ns.verify_failed_msg:
            attr = curses.color_pair(COLOR_DANGER) | curses.A_BOLD

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
