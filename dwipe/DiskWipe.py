"""
DiskWipe class - Main application controller/singleton
"""
# pylint: disable=invalid-name,broad-exception-caught,line-too-long
# pylint: disable=too-many-nested-blocks,too-many-instance-attributes
# pylint: disable=too-many-branches,too-many-statements,too-many-locals
# pylint: disable=protected-access,too-many-return-statements
# pylint: disable=too-few-public-methods
import os
import sys
import re
import time
import threading
# import shutil
import json
import curses as cs
from types import SimpleNamespace
from console_window import (ConsoleWindow, ConsoleWindowOpts, OptionSpinner,
            IncrementalSearchBar, InlineConfirmation, Theme,
            Screen, ScreenStack, Context)

from .WipeJob import WipeJob
from .DeviceInfo import DeviceInfo
from .Utils import Utils
from .PersistentState import PersistentState
from .StructuredLogger import StructuredLogger
from .LsblkMonitor import LsblkMonitor

# Screen constants
MAIN_ST = 0
HELP_ST = 1
LOG_ST = 2
THEME_ST = 3
SCREEN_NAMES = ('MAIN', 'HELP', 'HISTORY', 'THEMES')


class DiskWipe:
    """Main application controller and UI manager"""
    singleton = None

    def __init__(self, opts=None):
        DiskWipe.singleton = self
        self.opts = opts if opts else SimpleNamespace(debug=0)
        self.mounts_lines = None
        self.partitions = {}  # a dict of namespaces keyed by name
        self.wids = None
        self.job_cnt = 0
        self.exit_when_no_jobs = False

        # Per-device throttle tracking (keyed by device_path)
        # Values: {'mbps': int, 'auto': bool} or None
        self.device_throttles = {}

        self.prev_filter = ''  # string
        self.filter = None  # compiled pattern
        self.pick_is_running = False
        self.dev_info = None

        self.win, self.spin = None, None
        self.screens, self.stack = [], None

        # Inline confirmation handler
        self.confirmation = InlineConfirmation()

        # Incremental search bar for filtering
        self.filter_bar = IncrementalSearchBar(
            on_change=self._on_filter_change,
            on_accept=self._on_filter_accept,
            on_cancel=self._on_filter_cancel
        )

        # Initialize persistent state
        self.persistent_state = PersistentState()

    def _start_wipe(self):
        """Start the wipe job after confirmation"""
        if self.confirmation.identity and self.confirmation.identity in self.partitions:
            part = self.partitions[self.confirmation.identity]
            # Clear any previous verify failure message when starting wipe
            if hasattr(part, 'verify_failed_msg'):
                delattr(part, 'verify_failed_msg')

            # Get the wipe type from user's choice
            wipe_type = self.confirmation.input_buffer.strip()

            # Check if it's a firmware wipe
            if wipe_type not in ('Zero', 'Rand'):
                # Firmware wipe - check if it's available
                if not part.hw_caps or wipe_type not in part.hw_caps:
                    part.mounts = [f'⚠ Firmware wipe {wipe_type} not available']
                    self.confirmation.cancel()
                    self.win.passthrough_mode = False
                    return

                # Get command args from hw_caps
                command_args = part.hw_caps[wipe_type]

                # Import firmware task classes
                from .FirmwareWipeTask import NvmeWipeTask, SataWipeTask

                # Determine task type based on device name
                if part.name.startswith('nvme'):
                    task_class = NvmeWipeTask
                else:
                    task_class = SataWipeTask

                # Create firmware task
                task = task_class(
                    device_path=f'/dev/{part.name}',
                    total_size=part.size_bytes,
                    opts=self.opts,
                    command_args=command_args,
                    wipe_name=wipe_type
                )

                # Store wipe type for logging
                part.wipe_type = wipe_type

                # Create WipeJob with single firmware task
                part.job = WipeJob(
                    device_path=f'/dev/{part.name}',
                    total_size=part.size_bytes,
                    opts=self.opts,
                    tasks=[task]
                )
                part.job.thread = threading.Thread(target=part.job.run_tasks)
                part.job.thread.start()

                self.job_cnt += 1
                self.set_state(part, to='0%')

                # Clear confirmation and return early
                self.confirmation.cancel()
                self.win.passthrough_mode = False
                return

            # Construct full wipe mode (e.g., 'Zero+V', 'Rand', etc.)
            if self.opts.wipe_mode == '+V':
                full_wipe_mode = wipe_type + '+V'
            else:
                full_wipe_mode = wipe_type

            # Store wipe type for later logging
            part.wipe_type = wipe_type

            # Temporarily set the full wipe mode
            old_wipe_mode = self.opts.wipe_mode
            self.opts.wipe_mode = full_wipe_mode

            try:
                part.job = WipeJob.start_job(f'/dev/{part.name}',
                                              part.size_bytes, opts=self.opts)
                self.job_cnt += 1
                self.set_state(part, to='0%')
            finally:
                # Restore original wipe_mode
                self.opts.wipe_mode = old_wipe_mode

        # Clear confirmation state
        self.confirmation.cancel()
        self.win.passthrough_mode = False  # Disable passthrough

    def _start_verify(self):
        """Start the verify job after confirmation"""
        if self.confirmation.identity and self.confirmation.identity in self.partitions:
            part = self.partitions[self.confirmation.identity]
            # Clear any previous verify failure message when starting verify
            if hasattr(part, 'verify_failed_msg'):
                delattr(part, 'verify_failed_msg')
            part.job = WipeJob.start_verify_job(f'/dev/{part.name}',
                                                part.size_bytes, opts=self.opts)
            self.job_cnt += 1
        # Clear confirmation state
        self.confirmation.cancel()
        self.win.passthrough_mode = False  # Disable passthrough

    def test_state(self, ns, to=None):
        """Test if OK to set state of partition"""
        return self.dev_info.set_one_state(self.partitions, ns, test_to=to)

    def set_state(self, ns, to=None):
        """Set state of partition"""
        result = self.dev_info.set_one_state(self.partitions, ns, to=to)

        # Save block state changes to persistent state
        if result and to in ('Blk', 'Unbl'):
            self.persistent_state.set_device_locked(ns, to == 'Blk')

        return result

    def do_key(self, key):
        """Handle keyboard input"""
        if self.exit_when_no_jobs:
            # Check if all jobs are done and exit
            jobs_running = sum(1 for part in self.partitions.values() if part.job)
            if jobs_running == 0:
                self.win.stop_curses()
                os.system('clear; stty sane')
                sys.exit(0)
            return True  # continue running

        if not key:
            return True

        # Handle search bar input
        if self.stack.curr.num == LOG_ST:
            screen_obj = self.stack.get_curr_obj()
            if screen_obj.search_bar.is_active:
                if screen_obj.search_bar.handle_key(key):
                    return None # key handled by search bar

        # Handle filter bar input
        if self.filter_bar.is_active:
            if self.filter_bar.handle_key(key):
                return None  # Key was handled by filter bar

        # Handle confirmation mode input (wipe or verify)
        if self.confirmation.active:
            result = self.confirmation.handle_key(key)
            if result == 'confirmed':
                if self.confirmation.action_type == 'wipe':
                    self._start_wipe()
                elif self.confirmation.action_type == 'verify':
                    self._start_verify()
            elif result == 'cancelled':
                self.confirmation.cancel()
                self.win.passthrough_mode = False
            return None

        if key in (cs.KEY_ENTER, 10):  # Handle ENTER
            # ENTER pops screen (returns from help, etc.)
            if hasattr(self.spin, 'stack') and self.spin.stack.curr.num != MAIN_ST:
                self.spin.stack.pop()
                return None

        if key in self.spin.keys:
            _ = self.spin.do_key(key, self.win)
        return None

    def get_keys_line(self):
        """Generate the header line showing available keys"""
        # Get actions for the currently picked context
        _, pick_actions = self.get_actions(None)

        line = ''
        for key, verb in pick_actions.items():
            if key[0].lower() == verb[0].lower():
                # First letter matches - use [x]verb format
                line += f' [{verb[0]}]{verb[1:]}'
            else:
                # First letter doesn't match - use key:verb format
                line += f' {key}:{verb}'
        line += ' [S]top' if self.job_cnt > 0 else ''
        line = f'{line:<20} '
        line += self.filter_bar.get_display_string(prefix=' /') or ' /'
        # Show mode spinner with key
        line += f' [m]ode={self.opts.wipe_mode}'
        # Show passes spinner with key
        line += f' [P]ass={self.opts.passes}'
        # Show verification percentage spinner with key
        line += f' [V]pct={self.opts.verify_pct}%'
        line += f' [p]ort={self.opts.port_serial}'
        # line += ' !:scan [h]ist [t]heme ?:help [q]uit'
        line += ' [h]ist [t]heme ?:help [q]uit'
        return line[1:]

    def get_actions(self, part):
        """Determine the type of the current line and available commands."""
        name, actions = '', {}
        ctx = self.win.get_picked_context()
        if ctx and hasattr(ctx, 'partition'):
            part = ctx.partition
            name = part.name
            self.pick_is_running = bool(part.job)
            if self.test_state(part, to='STOP'):
                actions['s'] = 'stop'
            elif self.test_state(part, to='0%'):
                actions['w'] = 'wipe'
                if part.parent is None:
                    actions['DEL'] = 'DEL'
            # Can verify:
            # 1. Anything with wipe markers (states 's' or 'W')
            # 2. Unmarked whole disks (no parent, state '-' or '^') WITHOUT partitions that have filesystems
            # 3. Unmarked partitions without filesystems (has parent, state '-' or '^', no fstype)
            # 4. Only if verify_pct > 0
            # This prevents verifying filesystems which is nonsensical
            verify_pct = getattr(self.opts, 'verify_pct', 0)
            if not part.job and verify_pct > 0:
                if part.state in ('s', 'W'):
                    actions['v'] = 'verify'
                elif part.state in ('-', '^'):
                    # For whole disks (no parent), only allow verify if no partitions have filesystems
                    # For partitions, only allow if no filesystem
                    if not part.parent:
                        # Whole disk - check if any child partitions have filesystems
                        has_typed_partitions = any(
                            p.parent == part.name and p.fstype
                            for p in self.partitions.values()
                        )
                        if not has_typed_partitions:
                            actions['v'] = 'verify'
                    elif not part.fstype:
                        # Partition without filesystem
                        actions['v'] = 'verify'
            if self.test_state(part, to='Blk'):
                actions['b'] = 'block'
            if self.test_state(part, to='Unbl'):
                actions['b'] = 'unblk'
        return name, actions

    def _on_filter_change(self, text):
        """Callback when filter text changes - compile and apply filter in real-time"""
        text = text.strip()
        if not text:
            self.filter = None
            return

        try:
            self.filter = re.compile(text, re.IGNORECASE)
        except Exception:
            # Invalid regex - keep previous filter active
            pass

    def _on_filter_accept(self, text):
        """Callback when filter is accepted (ENTER pressed)"""
        self.prev_filter = text.strip()
        self.win.passthrough_mode = False
        # Move to top when filter is applied
        if text.strip():
            self.win.pick_pos = 0

    def _on_filter_cancel(self, original_text):
        """Callback when filter is cancelled (ESC pressed)"""
        # Restore original filter
        if original_text:
            self.filter = re.compile(original_text, re.IGNORECASE)
            self.prev_filter = original_text
        else:
            self.filter = None
            self.prev_filter = ''
        self.win.passthrough_mode = False

    def get_hw_caps_when_needed(self):
        """ Look for wipeable disks w/o hardware info """
        if not self.dev_info:
            return
        for  ns in self.partitions.values():
            if ns.parent:
                continue
            if ns.port.startswith('USB'):
                continue
            if ns.name[:2] not in ('nv', 'sd', 'hd'):
                continue
            if ns.hw_nopes or ns.hw_caps:  # already done
                continue
            if self.test_state(ns, to='0%'):
                self.dev_info.get_hw_capabilities(ns)


    def main_loop(self):
        """Main event loop"""

        # Create screen instances
        ThemeScreen = Theme.create_picker_screen(DiskWipeScreen)
        self.screens = {
            MAIN_ST: MainScreen(self),
            HELP_ST: HelpScreen(self),
            LOG_ST: HistoryScreen(self),
            THEME_ST: ThemeScreen(self),
        }

        # Create console window with custom pick highlighting
        win_opts = ConsoleWindowOpts(
            head_line=True,
            body_rows=200,
            head_rows=4,
            min_cols_rows=(60,10),
            # keys=self.spin.keys ^ other_keys,
            pick_attr=cs.A_REVERSE,  # Use reverse video for pick highlighting
            ctrl_c_terminates=False,
        )
        lsblk_monitor = LsblkMonitor(check_interval=0.2)
        lsblk_monitor.start()
        print("Starting first lsblk...")
        # Initialize device info and pick range before first draw
        info = DeviceInfo(opts=self.opts, persistent_state=self.persistent_state)
        lsblk_output = None
        while not lsblk_output:
            lsblk_output = lsblk_monitor.get_and_clear()
            self.partitions = info.assemble_partitions(self.partitions, lsblk_output)
            if lsblk_output:
                # print(lsblk_output, '\n\n')
                print('got ... got lsblk result')
                if self.opts.dump_lsblk:
                    DeviceInfo.dump(self.partitions, title="after assemble_partitions")
                    exit(1)
            time.sleep(0.2)

        self.win = ConsoleWindow(opts=win_opts)
        # Initialize screen stack
        self.stack = ScreenStack(self.win, None, SCREEN_NAMES, self.screens)

        spin = self.spin = OptionSpinner(stack=self.stack)
        spin.default_obj = self.opts
        spin.add_key('dense', 'D - dense/spaced view', vals=[False, True])
        spin.add_key('port_serial', 'p - disk port info', vals=['Auto', 'On', 'Off'])
        spin.add_key('slowdown_stop', 'W - stop if disk slows Nx', vals=[64, 256, 0, 4, 16])
        spin.add_key('stall_timeout', 'T - stall timeout (sec)', vals=[60, 120, 300, 600, 0,])
        spin.add_key('verify_pct', 'V - verification %', vals=[0, 2, 5, 10, 25, 50, 100])
        spin.add_key('passes', 'P - wipe pass count', vals=[1, 2, 4])
        spin.add_key('wipe_mode', 'm - wipe mode', vals=['-V', '+V'])

        spin.add_key('quit', 'q,x - quit program', keys='qx', genre='action')
        spin.add_key('screen_escape', 'ESC- back one screen',
                     keys=[10,27,cs.KEY_ENTER], genre='action')
        spin.add_key('main_escape', 'ESC - clear filter',
                     keys=27, genre='action', scope=MAIN_ST)
        spin.add_key('wipe', 'w - wipe device', genre='action')
        spin.add_key('verify', 'v - verify device', genre='action')
        spin.add_key('stop', 's - stop wipe', genre='action')
        spin.add_key('block', 'b - block/unblock disk', genre='action')
        spin.add_key('delete_device', 'DEL - remove disk from lsblk',
                         genre='action', keys=(cs.KEY_DC))
        spin.add_key('scan_all_devices', '! - rescan all devices', genre='action')
        spin.add_key('stop_all', 'S - stop ALL wipes', genre='action')
        spin.add_key('help', '? - show help screen', genre='action')
        spin.add_key('history', 'h - show wipe history', genre='action')
        spin.add_key('filter', '/ - filter devices by regex', genre='action')
        spin.add_key('theme_screen', 't - theme picker', genre='action', scope=MAIN_ST)
        spin.add_key('spin_theme', 't - theme', genre='action', scope=THEME_ST)
        spin.add_key('header_mode', '_ - header style', vals=['Underline', 'Reverse', 'Off'])
        spin.add_key('expand', 'e - expand history entry', genre='action', scope=LOG_ST)
        spin.add_key('show_keys', 'K - show keys (demo mode)', genre='action')
        self.opts.theme = ''
        self.persistent_state.restore_updated_opts(self.opts)
        Theme.set(self.opts.theme)
        self.win.set_handled_keys(self.spin.keys)

        # Start background lsblk monitor

        self.get_hw_caps_when_needed()
        self.dev_info = info
        pick_range = info.get_pick_range()
        self.win.set_pick_range(pick_range[0], pick_range[1])


        check_devices_mono = time.monotonic()

        try:
            while True:
                # Draw current screen
                current_screen = self.screens[self.stack.curr.num]
                current_screen.draw_screen()
                self.win.render()

                # Main thread timeout for responsive UI (background monitor checks every 0.2s)
                _ = self.do_key(self.win.prompt(seconds=0.25))

                # Handle actions using perform_actions
                self.stack.perform_actions(spin)

                # Check for new lsblk data from background monitor
                lsblk_output = lsblk_monitor.get_and_clear()
                time_since_refresh = time.monotonic() - check_devices_mono

                if lsblk_output or time_since_refresh > 3.0:
                    # Refresh if: device changes detected OR periodic refresh (3s default)
                    info = DeviceInfo(opts=self.opts, persistent_state=self.persistent_state)
                    self.partitions = info.assemble_partitions(self.partitions, lsblk_output=lsblk_output)
                    self.get_hw_caps_when_needed()
                    self.dev_info = info
                    # Update pick range to highlight NAME through SIZE fields
                    pick_range = info.get_pick_range()
                    self.win.set_pick_range(pick_range[0], pick_range[1])
                    check_devices_mono = time.monotonic()

                # Save any persistent state changes
                self.persistent_state.save_updated_opts(self.opts)
                self.persistent_state.sync()

                self.win.clear()
        finally:
            # Clean up monitor thread on exit
            lsblk_monitor.stop()

class DiskWipeScreen(Screen):
    """ TBD """
    app: DiskWipe
    refresh_seconds = 3.0  # Default refresh rate for screens

    def screen_escape_ACTION(self):
        """ return to main screen """
        self.app.stack.pop()

    def show_keys_ACTION(self):
        """ Show last key for demo"""
        self.app.win.set_demo_mode(enabled=None) # toggle it

class MainScreen(DiskWipeScreen):
    """Main device list screen"""

    def __init__(self, app):
        super().__init__(app)
        self.app = app
        self.persist_port_serial = set()


    def _port_serial_line(self, partition):
        wids = self.app.wids
        wid = wids.state if wids else 5
        sep, key_str = '  ', ''
        port, serial = partition.port, partition.serial
        if partition.hw_caps or partition.hw_nopes:
            lead = 'CAPS' if partition.hw_caps else 'ERRS'
            infos = partition.hw_caps if partition.hw_caps else partition.hw_nopes
            key_str = f'   Fw{lead}: ' + ','.join(list(infos.keys()))
        return f'{"":>{wid}}{sep}│   └────── {port:<12} {serial}{key_str}'

    def draw_screen(self):
        """Draw the main device list"""
        app = self.app

        def wanted(name):
            return not app.filter or app.filter.search(name)

        app.win.set_pick_mode(True)
        if app.opts.port_serial != 'Auto':
            self.persist_port_serial = set() # name of disks
        else: # if the disk goes away, clear persistence
            for name in list(self.persist_port_serial):
                if name not in app.partitions:
                    self.persist_port_serial.discard(name)

        # First pass: process jobs and collect visible partitions
        visible_partitions = []
        for name, partition in app.partitions.items():
            partition.line = None
            if partition.job:
                if partition.job.done:
                    # Join with timeout to avoid UI freeze if thread is stuck in blocking I/O
                    partition.job.thread.join(timeout=5.0)
                    if partition.job.thread.is_alive():
                        # Thread didn't exit cleanly - continue anyway to avoid UI freeze
                        # Leave job attached so we can try again next refresh
                        partition.mounts = ['⚠ Thread stuck, retrying...']
                        continue

                    # Check if this was a standalone verify job or a wipe job
                    is_verify_only = getattr(partition.job, 'is_verify_only', False)

                    if is_verify_only:
                        # Standalone verification completed or stopped
                        if partition.job.do_abort:
                            # Verification was stopped - read marker to get previous status
                            marker = WipeJob.read_marker_buffer(partition.name)
                            prev_status = getattr(marker, 'verify_status', None) if marker else None
                            if prev_status == 'pass':
                                was = '✓'
                            elif prev_status == 'fail':
                                was = '✗'
                            else:
                                was = '-'
                            partition.mounts = [f'Stopped verification, was {was}']
                        else:
                            # Verification completed successfully
                            verify_result = partition.job.verify_result or "unknown"
                            partition.mounts = [f'Verified: {verify_result}']

                            # Check if this was an unmarked disk/partition (no existing marker)
                            # Whole disks (no parent) or partitions without filesystems
                            was_unmarked = partition.dflt == '-' and (not partition.parent or not partition.fstype)

                            # Check if verification passed (may include debug info)
                            verify_passed = verify_result in ('zeroed', 'random') or verify_result.startswith(('zeroed', 'random'))

                            # If this was an unmarked disk that passed verification,
                            # update state to 'W' as if it had been wiped
                            if was_unmarked and verify_passed:
                                partition.state = 'W'
                                partition.dflt = 'W'
                                partition.wiped_this_session = True  # Show green
                                # Clear any previous verify failure
                                if hasattr(partition, 'verify_failed_msg'):
                                    delattr(partition, 'verify_failed_msg')
                            # If unmarked partition failed verification, set persistent error
                            # NOTE: Only for unmarked disks - marked disks just show ✗ in marker
                            elif was_unmarked and not verify_passed:
                                error_msg = '⚠ VERIFY FAILED: Not wiped w/ Zero or Rand'
                                partition.mounts = [error_msg]
                                partition.verify_failed_msg = error_msg
                            else:
                                # Marked disk or other case - clear verify failure
                                if hasattr(partition, 'verify_failed_msg'):
                                    delattr(partition, 'verify_failed_msg')

                            # Log the verify operation
                            if partition.job.verify_start_mono:
                                elapsed = time.monotonic() - partition.job.verify_start_mono

                                # Determine if verification passed or failed
                                if verify_result in ('zeroed', 'random') or verify_result.startswith('random ('):
                                    result = 'OK'
                                    verify_detail = None
                                elif verify_result == 'error':
                                    result = 'FAIL'
                                    verify_detail = 'error'
                                elif verify_result == 'skipped':
                                    result = 'skip'
                                    verify_detail = None
                                else:
                                    # Failed verification - extract reason
                                    result = 'FAIL'
                                    # verify_result like "not-wiped (non-zero at 22K)" or "not-wiped (max=5.2%)"
                                    if '(' in verify_result:
                                        verify_detail = verify_result.split('(')[1].rstrip(')')
                                    else:
                                        verify_detail = verify_result

                                # Structured logging
                                Utils.log_wipe_structured(app.partitions, partition, partition.job)
                                # Legacy text log (keep for compatibility)
                                Utils.log_wipe(partition.name, partition.size_bytes, 'Vrfy', result, elapsed,
                                              uuid=partition.uuid, verify_result=verify_detail)
                        app.job_cnt -= 1
                        # Reset state back to default (was showing percentage during verify)
                        # Unless we just updated it above for unmarked verified disk
                        if partition.state.endswith('%'):
                            partition.state = partition.dflt
                        partition.job = None
                        partition.marker_checked = False  # Reset to "dont-know" - will re-read on next scan
                        partition.marker = ''  # Clear stale marker string to avoid showing old data during re-read
                    else:
                        # Wipe job completed (with or without auto-verify)
                        # Check if stopped during verify phase (after successful write)
                        if partition.job.do_abort and partition.job.verify_phase:
                            # Wipe completed but verification was stopped
                            to = 'W'
                            app.set_state(partition, to=to)
                            partition.dflt = to
                            partition.wiped_this_session = True
                            # Read marker to get previous verification status
                            marker = WipeJob.read_marker_buffer(partition.name)
                            prev_status = getattr(marker, 'verify_status', None) if marker else None
                            if prev_status == 'pass':
                                was = '✓'
                            elif prev_status == 'fail':
                                was = '✗'
                            else:
                                was = '-'
                            partition.mounts = [f'Stopped verification, was {was}']
                        else:
                            # Normal wipe completion or stopped during write
                            to = 's' if partition.job.do_abort else 'W'
                            app.set_state(partition, to=to)
                            partition.dflt = to
                            # Mark as wiped in this session (for green highlighting)
                            if to == 'W':
                                partition.wiped_this_session = True
                            partition.mounts = []
                        app.job_cnt -= 1
                        # Log the wipe operation
                        elapsed = time.monotonic() - partition.job.start_mono
                        result = 'stopped' if partition.job.do_abort else 'completed'
                        # Get the wipe type that was used (stored when wipe was started)
                        mode = getattr(partition, 'wipe_type', 'Unknown')
                        # Calculate percentage if stopped
                        pct = None
                        if partition.job.do_abort and partition.job.total_size > 0:
                            pct = int((partition.job.total_written / partition.job.total_size) * 100)
                        # Structured logging
                        Utils.log_wipe_structured(app.partitions, partition, partition.job, mode=mode)
                        # Legacy text log (keep for compatibility)
                        # Only pass label/fstype for stopped wipes (not completed)
                        if result == 'stopped':
                            Utils.log_wipe(partition.name, partition.size_bytes, mode, result, elapsed,
                                          uuid=partition.uuid, label=partition.label, fstype=partition.fstype, pct=pct)
                        else:
                            Utils.log_wipe(partition.name, partition.size_bytes, mode, result, elapsed,
                                          uuid=partition.uuid, pct=pct)

                        # Log auto-verify if it happened (verify_result will be set)
                        if partition.job.verify_result and partition.job.verify_start_mono:
                            verify_elapsed = time.monotonic() - partition.job.verify_start_mono
                            verify_result = partition.job.verify_result

                            # Determine if verification passed or failed
                            if verify_result in ('zeroed', 'random') or verify_result.startswith('random ('):
                                result = 'OK'
                                verify_detail = None
                            elif verify_result == 'error':
                                result = 'FAIL'
                                verify_detail = 'error'
                            elif verify_result == 'skipped':
                                result = 'skip'
                                verify_detail = None
                            else:
                                # Failed verification - extract reason
                                result = 'FAIL'
                                # verify_result like "not-wiped (non-zero at 22K)" or "not-wiped (max=5.2%)"
                                if '(' in verify_result:
                                    verify_detail = verify_result.split('(')[1].rstrip(')')
                                else:
                                    verify_detail = verify_result

                            # Note: Structured logging for verify was already logged above as part of the wipe
                            # This is just logging the separate verify phase stats to the legacy log
                            Utils.log_wipe(partition.name, partition.size_bytes, 'Vrfy', result, verify_elapsed,
                                          uuid=partition.uuid, verify_result=verify_detail)

                        if partition.job.exception:
                            app.win.alert(
                                message=f'FAILED: wipe {repr(partition.name)}\n{partition.job.exception}',
                                title='ALERT'
                            )

                        partition.job = None
                        partition.marker_checked = False  # Reset to "dont-know" - will re-read on next scan
                        partition.marker = ''  # Clear stale marker string to avoid showing old data during re-read
            if partition.job:
                elapsed, pct, rate, until = partition.job.get_status()

                # Get task display name (Zero, Rand, Crypto, Verify, etc.)
                task_name = ""
                if partition.job.current_task:
                    task_name = partition.job.current_task.get_display_name()

                # FLUSH goes in mounts column, not state
                if pct.startswith('FLUSH'):
                    partition.state = partition.dflt  # Keep default state (s, W, etc)
                    if rate and until:
                        partition.mounts = [f'{task_name} {pct} {elapsed} -{until} {rate}']
                    else:
                        partition.mounts = [f'{task_name} {pct} {elapsed}']
                else:
                    partition.state = pct
                    # Build progress line with task name
                    progress_parts = [task_name, elapsed, f'-{until}', rate]

                    # Only show slowdown/stall if job tracks these metrics
                    # (WriteTask does, VerifyTask and FirmwareWipeTask don't)
                    if hasattr(partition.job, 'max_slowdown_ratio') and hasattr(partition.job, 'max_stall_secs'):
                        slowdown = partition.job.max_slowdown_ratio
                        stall = partition.job.max_stall_secs
                        progress_parts.extend([f'÷{slowdown}', f'𝚫{Utils.ago_str(stall)}'])

                    partition.mounts = [' '.join(progress_parts)]

            if partition.parent and partition.parent in app.partitions and (
                    app.partitions[partition.parent].state == 'Blk'):
                continue

            if wanted(name) or partition.job:
                visible_partitions.append(partition)

        # Re-infer parent states (like 'Busy') after updating child job states
        DeviceInfo.set_all_states(app.partitions)

        # Build mapping of parent -> last visible child
        parent_last_child = {}
        for partition in visible_partitions:
            if partition.parent:
                parent_last_child[partition.parent] = partition.name

        # Second pass: display visible partitions with tree characters and Context
        prev_disk = None
        for partition in visible_partitions:
            # Add separator line between disk groups (unless in dense mode)
            if not app.opts.dense and partition.parent is None and prev_disk is not None:
                # Add dimmed separator line between disks
                separator = '─' * app.win.get_pad_width()
                app.win.add_body(separator, attr=cs.A_DIM, context=Context(genre='DECOR'))

            if partition.parent is None:
                prev_disk = partition.name

            is_last_child = False
            if partition.parent and partition.parent in parent_last_child:
                is_last_child = bool(parent_last_child[partition.parent] == partition.name)

            partition.line, attr = app.dev_info.part_str(partition, is_last_child=is_last_child)
            # Create context with partition reference
            ctx = Context(genre='disk' if partition.parent is None else 'partition',
                         partition=partition)
            app.win.add_body(partition.line, attr=attr, context=ctx)
            if partition.parent is None and app.opts.port_serial != 'Off':
                doit = bool(app.opts.port_serial == 'On')
                if not doit:
                    doit = bool(partition.name in self.persist_port_serial)
                if not doit and app.test_state(partition, to='0%'):
                    doit = True
                    self.persist_port_serial.add(partition.name)
                if doit:
                    line = self._port_serial_line(partition)
                    app.win.add_body(line, attr=attr, context=Context(genre='DECOR'))

            # Show inline confirmation prompt if this is the partition being confirmed
            if app.confirmation.active and app.confirmation.identity == partition.name:
                # Build confirmation message
                if app.confirmation.action_type == 'wipe':
                    msg = f'⚠️  WIPE {partition.name}'
                else:  # verify
                    msg = f'⚠️  VERIFY {partition.name} [writes marker]'

                # Add mode-specific prompt (base message without input)
                if app.confirmation.mode == 'yes':
                    msg += " - Type 'yes': "
                elif app.confirmation.mode == 'identity':
                    msg += f" - Type '{partition.name}': "
                elif app.confirmation.mode == 'choices':
                    choices_str = ', '.join(app.confirmation.choices)
                    msg += f" - Choose ({choices_str}): "

                # Position message at fixed column (reduced from 28 to 20)
                msg = ' ' * 20 + msg

                # Add confirmation message base as DECOR (non-pickable)
                app.win.add_body(msg, attr=cs.color_pair(Theme.DANGER) | cs.A_BOLD,
                               context=Context(genre='DECOR'))

                # Add input or hint on same line
                if app.confirmation.input_buffer:
                    # Show current input with cursor
                    app.win.add_body(app.confirmation.input_buffer + '_',
                                   attr=cs.color_pair(Theme.DANGER) | cs.A_BOLD,
                                   resume=True)
                else:
                    # Show hint in dimmed italic
                    hint = app.confirmation.get_hint()
                    app.win.add_body(hint, attr=cs.A_DIM | cs.A_ITALIC, resume=True)

        app.win.add_fancy_header(app.get_keys_line(), mode=app.opts.header_mode)

        app.win.add_header(app.dev_info.head_str, attr=cs.A_DIM)
        _, col = app.win.head.pad.getyx()
        pad = ' ' * (app.win.get_pad_width() - col)
        app.win.add_header(pad, resume=True, attr=cs.A_DIM)

    ######################################### ACTIONS #####################
    @staticmethod
    def clear_hotswap_marker(part):
        """Clear the hot-swap marker (^) when user performs a hard action"""
        if part.state == '^':
            part.state = '-'
            # Also update dflt so verify/wipe operations restore to '-' not '^'
            part.dflt = '-'
        # Also clear the newly_inserted flag
        if hasattr(part, 'newly_inserted'):
            delattr(part, 'newly_inserted')

    def main_escape_ACTION(self):
        """ Handle ESC clearing filter and move to top"""
        app = self.app
        app.prev_filter = ''
        app.filter = None
        app.filter_bar._text = ''  # Also clear filter bar text
        app.win.pick_pos = 0

    def theme_screen_ACTION(self):
        """ handle 't' from Main Screen """
        self.app.stack.push(THEME_ST, self.app.win.pick_pos)

    def quit_ACTION(self):
        """Handle quit action (q or x key pressed)"""
        app = self.app

        def stop_if_idle(part):
            if part.state[-1] == '%':
                if part.job and not part.job.done:
                    part.job.do_abort = True
            return 1 if part.job else 0

        def stop_all():
            rv = 0
            for part in app.partitions.values():
                rv += stop_if_idle(part)
            return rv

        def exit_if_no_jobs():
            if stop_all() == 0:
                app.win.stop_curses()
                os.system('clear; stty sane')
                sys.exit(0)

        app.exit_when_no_jobs = True
        app.filter = re.compile('STOPPING', re.IGNORECASE)
        app.prev_filter = 'STOPPING'
        app.filter_bar._text = 'STOPPING'  # Update filter bar display
        exit_if_no_jobs()

    def wipe_ACTION(self):
        """Handle 'w' key"""
        app = self.app
        if not app.pick_is_running:
            ctx = app.win.get_picked_context()
            if ctx and hasattr(ctx, 'partition'):
                part = ctx.partition
                if app.test_state(part, to='0%'):
                    self.clear_hotswap_marker(part)
                    # Build choices: Zero, Rand, and any firmware wipe types
                    choices = ['Zero', 'Rand']
                    if part.hw_caps:
                        choices.extend(list(part.hw_caps.keys()))
                    app.confirmation.start(action_type='wipe',
                               identity=part.name, mode='choices', choices=choices)
                    app.win.passthrough_mode = True

    def verify_ACTION(self):
        """Handle 'v' key"""
        app = self.app
        ctx = app.win.get_picked_context()
        if ctx and hasattr(ctx, 'partition'):
            part = ctx.partition
            # Use get_actions() to ensure we use the same logic as the header display
            _, actions = app.get_actions(part)
            if 'v' in actions:
                self.clear_hotswap_marker(part)
                # Check if this is an unmarked disk/partition (potential data loss risk)
                # Whole disks (no parent) or partitions without filesystems need confirmation
                is_unmarked = part.state == '-' and (not part.parent or not part.fstype)
                if is_unmarked:
                    # Require confirmation for unmarked partitions
                    app.confirmation.start(action_type='verify',
                                           identity=part.name, mode="yes")
                    app.win.passthrough_mode = True
                else:
                    # Marked partition - proceed directly
                    # Clear any previous verify failure message when starting new verify
                    if hasattr(part, 'verify_failed_msg'):
                        delattr(part, 'verify_failed_msg')
                    part.job = WipeJob.start_verify_job(f'/dev/{part.name}',
                                                        part.size_bytes, opts=app.opts)
                    app.job_cnt += 1

    def scan_all_devices_ACTION(self):
        """ Trigger a re-scan of all devices to make the appear
        quicker in the list"""
        base_path = '/sys/class/scsi_host'
        if not os.path.exists(base_path):
            return
        for host in os.listdir(base_path):
            scan_file = os.path.join(base_path, host, 'scan')
            if os.path.exists(scan_file):
                try:
                    with open(scan_file, 'w', encoding='utf-8') as f:
                        f.write("- - -")
                except Exception:
                    pass

    def delete_device_ACTION(self):
        """ DEL key -- Cause the OS to drop a sata device so it
            can be replaced sooner """
        app = self.app
        ctx = app.win.get_picked_context()
        if ctx and hasattr(ctx, 'partition'):
            part = ctx.partition
            if not part or part.parent or not app.test_state(part, to='0%'):
                return
            path = f"/sys/block/{part.name}/device/delete"
            if os.path.exists(path):
                try:
                    with open(path, 'w', encoding='utf-8') as f:
                        f.write("1")
                    return True
                except Exception:
                    pass

    def stop_ACTION(self):
        """Handle 's' key"""
        app = self.app
        if app.pick_is_running:
            ctx = app.win.get_picked_context()
            if ctx and hasattr(ctx, 'partition'):
                part = ctx.partition
                if part.state[-1] == '%':
                    if part.job and not part.job.done:
                        part.job.do_abort = True


    def stop_all_ACTION(self):
        """Handle 'S' key"""
        app = self.app
        for part in app.partitions.values():
            if part.state[-1] == '%':
                if part.job and not part.job.done:
                    part.job.do_abort = True

    def block_ACTION(self):
        """Handle 'b' key"""
        app = self.app
        ctx = app.win.get_picked_context()
        if ctx and hasattr(ctx, 'partition'):
            part = ctx.partition
            self.clear_hotswap_marker(part)
            app.set_state(part, 'Unbl' if part.state == 'Blk' else 'Blk')

    def help_ACTION(self):
        """Handle '?' key - push help screen"""
        app = self.app
        if hasattr(app, 'spin') and hasattr(app.spin, 'stack'):
            app.spin.stack.push(HELP_ST, app.win.pick_pos)

    def history_ACTION(self):
        """Handle 'h' key - push history screen"""
        app = self.app
        if hasattr(app, 'spin') and hasattr(app.spin, 'stack'):
            app.spin.stack.push(LOG_ST, app.win.pick_pos)

    def filter_ACTION(self):
        """Handle '/' key - start incremental filter search"""
        app = self.app
        app.filter_bar.start(app.prev_filter)
        app.win.passthrough_mode = True


class HelpScreen(DiskWipeScreen):
    """Help screen"""

    def draw_screen(self):
        """Draw the help screen"""
        app = self.app
        spinner = self.get_spinner()

        app.win.set_pick_mode(False)
        if spinner:
            spinner.show_help_nav_keys(app.win)
            spinner.show_help_body(app.win)



class HistoryScreen(DiskWipeScreen):
    """History/log screen showing structured log entries with expand/collapse functionality"""

    refresh_seconds = 60.0  # Slower refresh for history screen to allow copy/paste

    def __init__(self, app):
        super().__init__(app)
        self.expands = {}  # Maps timestamp -> True (expanded) or False (collapsed)
        self.entries = []  # Cached log entries (all entries before filtering)
        self.filtered_entries = []  # Entries after search filtering
        self.window_of_logs = None  # Window of log entries (OrderedDict)
        self.window_state = None  # Window state for incremental reads
        self.search_matches = set()  # Set of timestamps with deep-only matches in JSON
        self.prev_filter = ''

        # Setup search bar
        self.search_bar = IncrementalSearchBar(
            on_change=self._on_search_change,
            on_accept=self._on_search_accept,
            on_cancel=self._on_search_cancel
        )

    def _on_search_change(self, text):
        """Called when search text changes - filter entries incrementally."""
        self._filter_entries(text)

    def _on_search_accept(self, text):
        """Called when ENTER pressed in search - keep filter active, exit input mode."""
        self.app.win.passthrough_mode = False
        self.prev_filter = text

    def _on_search_cancel(self, original_text):
        """Called when ESC pressed in search - restore and exit search mode."""
        self._filter_entries(original_text)
        self.app.win.passthrough_mode = False

    def _filter_entries(self, search_text):
        """Filter entries based on search text (shallow or deep)."""
        if not search_text:
            self.filtered_entries = self.entries
            self.search_matches = set()
            return

        # Deep search mode if starts with /
        deep_search = search_text.startswith('/')
        pattern = search_text[1:] if deep_search else search_text

        if not pattern:
            self.filtered_entries = self.entries
            self.search_matches = set()
            return

        # Use StructuredLogger's filter method
        # logger = Utils.get_logger()
        self.filtered_entries, self.search_matches = StructuredLogger.filter_entries(
            self.entries, pattern, deep=deep_search
        )

    def draw_screen(self):
        """Draw the history screen with structured log entries"""
        app = self.app
        win = app.win
        win.set_pick_mode(True)

        # Get window of log entries (chronological order - eldest to youngest)
        logger = Utils.get_logger()
        if self.window_of_logs is None:
            self.window_of_logs, self.window_state = logger.get_window_of_entries(window_size=1000)
        else:
            # Refresh window with any new entries
            self.window_of_logs, self.window_state = logger.refresh_window(
                self.window_of_logs, self.window_state, window_size=1000
            )

        # Convert to list in reverse order (newest first for display)
        self.entries = list(reversed(list(self.window_of_logs.values())))

        # Clean up self.expands: remove any timestamps that are no longer in entries
        valid_timestamps = {entry.timestamp for entry in self.entries}
        self.expands = {ts: state for ts, state in self.expands.items() if ts in valid_timestamps}

        # Apply search filter if active
        if not self.search_bar.text:
            self.filtered_entries = self.entries
            self.search_matches = set()

        # Count by level in filtered results
        level_counts = {}
        for e in self.filtered_entries:
            level_counts[e.level] = level_counts.get(e.level, 0) + 1

        # Build search display string
        search_display = self.search_bar.get_display_string(prefix='', suffix='')

        # Build level summary for header
        # level_summary = ' '.join(f'{lvl}:{cnt}' for lvl, cnt in sorted(level_counts.items()))

        # Header
        # header_line = f'ESC:back [e]xpand [/]search {len(self.filtered_entries)}/{len(self.entries)} ({level_summary}) '
        header_line = f'ESC:back [e]xpand [/]search {len(self.filtered_entries)}/{len(self.entries)} '
        if search_display:
            header_line += f'/ {search_display}'
        else:
            header_line += '/'
        win.add_header(header_line)
        win.add_header(f'Log: {logger.log_file}')

        # Build display
        for entry in self.filtered_entries:
            timestamp = entry.timestamp

            # Get display summary from entry
            summary = entry.display_summary

            # Format timestamp (just date and time)
            timestamp_display = timestamp[:19]  # YYYY-MM-DD HH:MM:SS

            level = entry.level

            # Add deep match indicator if this entry matched only in JSON
            deep_indicator = " *" if timestamp in self.search_matches else ""

            # Choose color based on log level
            if level == 'ERR':
                level_attr = cs.color_pair(Theme.ERROR) | cs.A_BOLD
            elif level in ('WIPE_STOPPED', 'VERIFY_STOPPED'):
                level_attr = cs.color_pair(Theme.WARNING) | cs.A_BOLD
            elif level in ('WIPE_COMPLETE', 'VERIFY_COMPLETE'):
                level_attr = cs.color_pair(Theme.SUCCESS) | cs.A_BOLD
            else:
                level_attr = cs.A_BOLD

            line = f"{timestamp_display} {summary}{deep_indicator}"
            win.add_body(line, attr=level_attr, context=Context("header", timestamp=timestamp))

            # Handle expansion - show the structured data
            if self.expands.get(timestamp, False):
                # Show the full entry data as formatted JSON
                try:
                    data_dict = entry.to_dict()
                    # Format just the 'data' field if it exists, otherwise show all
                    if 'data' in data_dict and data_dict['data']:
                        formatted = json.dumps(data_dict['data'], indent=2)
                    else:
                        formatted = json.dumps(data_dict, indent=2)

                    lines = formatted.split('\n')
                    for line in lines:
                        win.add_body(f"  {line}", context=Context("body", timestamp=timestamp))

                except Exception as e:
                    win.add_body(f"  (error formatting: {e})", attr=cs.A_DIM)

            # Empty line between entries
            win.add_body("", context=Context("DECOR"))

    def expand_ACTION(self):
        """'e' key - Expand/collapse current entry"""
        app = self.app
        win = app.win
        ctx = win.get_picked_context()

        if ctx and hasattr(ctx, 'timestamp'):
            timestamp = ctx.timestamp
            # Toggle between collapsed and expanded
            current = self.expands.get(timestamp, False)
            if current:
                del self.expands[timestamp]  # Collapse
            else:
                self.expands[timestamp] = True  # Expand

    def filter_ACTION(self):
        """'/' key - Start incremental search"""
        app = self.app
        self.search_bar.start(self.prev_filter)
        app.win.passthrough_mode = True
