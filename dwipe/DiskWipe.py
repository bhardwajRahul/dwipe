"""
DiskWipe class - Main application controller/singleton
"""
# pylint: disable=invalid-name,broad-exception-caught,line-too-long
# pylint: disable=too-many-nested-blocks,too-many-instance-attributes
# pylint: disable=too-many-branches,too-many-statements,too-many-locals
# pylint: disable=protected-access,too-many-return-statements
import os
import sys
import re
import time
import shutil
import curses as cs
from types import SimpleNamespace
from .ConsoleWindow import (ConsoleWindow, ConsoleWindowOpts, OptionSpinner,
                            IncrementalSearchBar,
                            COLOR_DANGER, COLOR_SUCCESS, COLOR_WARNING, COLOR_INFO,
                            COLOR_EMPHASIS, COLOR_ERROR, COLOR_PROGRESS, COLOR_HOTSWAP,
                            list_themes, get_current_theme, set_theme,
                            Screen, ScreenStack, THEMES, Context)

from .WipeJob import WipeJob
from .DeviceInfo import DeviceInfo
from .Utils import Utils
from .PersistentState import PersistentState

# Screen constants
MAIN_ST = 0
HELP_ST = 1
LOG_ST = 2
THEME_ST = 3
SCREEN_NAMES = ('MAIN', 'HELP', 'HISTORY', 'THEMES')


class InlineConfirmation:
    """Manages inline confirmation prompts for wipe/verify operations"""

    def __init__(self):
        self.active = False
        self.confirm_type = None  # 'wipe' or 'verify'
        self.partition_name = None
        self.input_buffer = ''
        self.mode = None  # 'Y', 'y', 'YES', 'yes', 'device'

    def start(self, confirm_type, partition_name, mode):
        """Start a confirmation prompt"""
        self.active = True
        self.confirm_type = confirm_type
        self.partition_name = partition_name
        self.input_buffer = ''
        self.mode = mode

    def cancel(self):
        """Cancel the confirmation"""
        self.active = False
        self.confirm_type = None
        self.partition_name = None
        self.input_buffer = ''

    def get_expected(self):
        """Get the expected input string"""
        if self.mode == 'device':
            return self.partition_name
        return self.mode

    def is_single_key(self):
        """Check if this mode uses single key confirmation"""
        return self.mode in ('Y', 'y')

    def handle_key(self, key):
        """Handle a key press during confirmation.

        Returns:
            'confirmed' - user confirmed
            'cancelled' - user cancelled (ESC)
            'continue' - still gathering input
        """
        if key == 27:  # ESC
            return 'cancelled'

        if self.is_single_key():
            # Single key confirmation
            expected = self.get_expected()
            if key == ord(expected):
                return 'confirmed'
            return 'continue'

        # Typed confirmation
        if 32 <= key <= 126:  # Printable ASCII
            self.input_buffer += chr(key)
        elif key in (cs.KEY_BACKSPACE, 127, 8):  # Backspace
            self.input_buffer = self.input_buffer[:-1]
        elif key in (cs.KEY_ENTER, 10):  # ENTER
            if self.input_buffer == self.get_expected():
                return 'confirmed'
            # Wrong input - reset
            self.input_buffer = ''
        return 'continue'


class DiskWipe:
    """Main application controller and UI manager"""
    singleton = None

    def __init__(self, opts=None):
        DiskWipe.singleton = self
        self.opts = opts if opts else SimpleNamespace(debug=0, dry_run=False)
        self.DB = bool(self.opts.debug)
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
        self.check_preqreqs()

    @staticmethod
    def check_preqreqs():
        """Check that needed programs are installed."""
        ok = True
        for prog in 'lsblk'.split():
            if shutil.which(prog) is None:
                ok = False
                print(f'ERROR: cannot find {prog!r} on $PATH')
        if not ok:
            sys.exit(1)

    def _start_wipe(self):
        """Start the wipe job after confirmation"""
        if self.confirmation.partition_name and self.confirmation.partition_name in self.partitions:
            part = self.partitions[self.confirmation.partition_name]
            # Clear any previous verify failure message when starting wipe
            if hasattr(part, 'verify_failed_msg'):
                delattr(part, 'verify_failed_msg')
            part.job = WipeJob.start_job(f'/dev/{part.name}',
                                          part.size_bytes, opts=self.opts)
            self.job_cnt += 1
            self.set_state(part, to='0%')
        # Clear confirmation state
        self.confirmation.cancel()
        self.win.passthrough_mode = False  # Disable passthrough

    def _start_verify(self):
        """Start the verify job after confirmation"""
        if self.confirmation.partition_name and self.confirmation.partition_name in self.partitions:
            part = self.partitions[self.confirmation.partition_name]
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

        # Save lock state changes to persistent state
        if result and to in ('Lock', 'Unlk'):
            self.persistent_state.set_device_locked(ns, to == 'Lock')

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

        # Handle filter bar input
        if self.filter_bar.is_active:
            if self.filter_bar.handle_key(key):
                return None  # Key was handled by filter bar

        # Handle confirmation mode input (wipe or verify)
        if self.confirmation.active:
            result = self.confirmation.handle_key(key)
            if result == 'confirmed':
                if self.confirmation.confirm_type == 'wipe':
                    self._start_wipe()
                elif self.confirmation.confirm_type == 'verify':
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
        line += '  '
        if self.opts.dry_run:
            line += ' DRY-RUN'
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
            if self.test_state(part, to='Lock'):
                actions['l'] = 'lock'
            if self.test_state(part, to='Unlk'):
                actions['l'] = 'unlk'
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

    def main_loop(self):
        """Main event loop"""

        # Create screen instances
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
            # keys=self.spin.keys ^ other_keys,
            pick_attr=cs.A_REVERSE,  # Use reverse video for pick highlighting
            ctrl_c_terminates=False,
        )

        self.win = ConsoleWindow(opts=win_opts)
        # Initialize screen stack
        self.stack = ScreenStack(self.win, None, SCREEN_NAMES, self.screens)

        spin = self.spin = OptionSpinner(stack=self.stack)
        spin.default_obj = self.opts
        spin.add_key('dense', 'D - dense/spaced view', vals=[False, True])
        spin.add_key('slowdown_stop', 'L - stop if disk slows Nx', vals=[16, 64, 256, 0, 4])
        spin.add_key('stall_timeout', 'T - stall timeout (sec)', vals=[60, 120, 300, 600, 0,])
        spin.add_key('verify_pct', 'V - verification %', vals=[0, 2, 5, 10, 25, 50, 100])
        spin.add_key('passes', 'P - wipe pass count', vals=[1, 2, 4])
        spin.add_key('wipe_mode', 'm - wipe mode', vals=['Zero', 'Zero+V', 'Rand', 'Rand+V'])
        spin.add_key('confirmation', 'c - confirmation mode', vals=['YES', 'yes', 'device', 'Y', 'y'])

        spin.add_key('quit', 'q,x - quit program', keys='qx', genre='action')
        spin.add_key('screen_escape', 'ESC- back one screen',
                     keys=[10,27,cs.KEY_ENTER], genre='action')
        spin.add_key('main_escape', 'ESC - clear filter',
                     keys=27, genre='action', scope=MAIN_ST)
        spin.add_key('wipe', 'w - wipe device', genre='action')
        spin.add_key('verify', 'v - verify device', genre='action')
        spin.add_key('stop', 's - stop wipe', genre='action')
        spin.add_key('lock', 'l - lock/unlock disk', genre='action')
        spin.add_key('stop_all', 'S - stop ALL wipes', genre='action')
        spin.add_key('help', '? - show help screen', genre='action')
        spin.add_key('history', 'h - show wipe history', genre='action')
        spin.add_key('filter', '/ - filter devices by regex', genre='action')
        spin.add_key('theme_screen', 't - theme picker', genre='action', scope=MAIN_ST)
        spin.add_key('spin_theme', 't - theme', genre='action', scope=THEME_ST); # vals=list_themes())
        spin.add_key('header_mode', '_ - header style', vals=['Underline', 'Reverse', 'Off'])
        self.opts.theme = ''
        self.persistent_state.restore_updated_opts(self.opts)
        set_theme(self.opts.theme)
        self.win.set_handled_keys(self.spin.keys)


        # self.opts.name = "[hit 'n' to enter name]"

        # Initialize device info and pick range before first draw
        info = DeviceInfo(opts=self.opts, persistent_state=self.persistent_state)
        self.partitions = info.assemble_partitions(self.partitions)
        self.dev_info = info
        pick_range = info.get_pick_range()
        self.win.set_pick_range(pick_range[0], pick_range[1])

        check_devices_mono = time.monotonic()
        while True:
            # Draw current screen
            current_screen = self.screens[self.stack.curr.num]
            current_screen.draw_screen()
            self.win.render()

            seconds = 3.0
            _ = self.do_key(self.win.prompt(seconds=seconds))

            # Handle actions using perform_actions
            self.stack.perform_actions(spin)

            if time.monotonic() - check_devices_mono > (seconds * 0.95):
                info = DeviceInfo(opts=self.opts, persistent_state=self.persistent_state)
                self.partitions = info.assemble_partitions(self.partitions)
                self.dev_info = info
                # Update pick range to highlight NAME through SIZE fields
                pick_range = info.get_pick_range()
                self.win.set_pick_range(pick_range[0], pick_range[1])
                check_devices_mono = time.monotonic()

            # Save any persistent state changes
            self.persistent_state.save_updated_opts(self.opts)
            self.persistent_state.sync()

            self.win.clear()

class DiskWipeScreen(Screen):
    """ TBD """
    app: DiskWipe

    def screen_escape_ACTION(self):
        """ return to main screen """
        self.app.stack.pop()

class MainScreen(DiskWipeScreen):
    """Main device list screen"""


    def draw_screen(self):
        """Draw the main device list"""
        app = self.app

        def wanted(name):
            return not app.filter or app.filter.search(name)

        app.win.set_pick_mode(True)

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

                                Utils.log_wipe(partition.name, partition.size_bytes, 'Vrfy', result, elapsed,
                                              uuid=partition.uuid, verify_result=verify_detail)
                        app.job_cnt -= 1
                        # Reset state back to default (was showing percentage during verify)
                        # Unless we just updated it above for unmarked verified disk
                        if partition.state.endswith('%'):
                            partition.state = partition.dflt
                        partition.job = None
                        partition.marker_checked = False  # Reset to "dont-know" - will re-read on next scan
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
                        # Extract base mode (remove '+V' suffix if present)
                        mode = app.opts.wipe_mode.replace('+V', '')
                        # Calculate percentage if stopped
                        pct = None
                        if partition.job.do_abort and partition.job.total_size > 0:
                            pct = int((partition.job.total_written / partition.job.total_size) * 100)
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

                            Utils.log_wipe(partition.name, partition.size_bytes, 'Vrfy', result, verify_elapsed,
                                          uuid=partition.uuid, verify_result=verify_detail)

                        if partition.job.exception:
                            app.win.stop_curses()
                            print('\n\n\n==========   ALERT  =========\n')
                            print(f' FAILED: wipe {repr(partition.name)}')
                            print(partition.job.exception)
                            input('\n\n===== Press ENTER to continue ====> ')
                            app.win._start_curses()

                        partition.job = None
                        partition.marker_checked = False  # Reset to "dont-know" - will re-read on next scan
            if partition.job:
                elapsed, pct, rate, until = partition.job.get_status()

                # FLUSH goes in mounts column, not state
                if pct.startswith('FLUSH'):
                    partition.state = partition.dflt  # Keep default state (s, W, etc)
                    if rate and until:
                        partition.mounts = [f'{pct} {elapsed} -{until} {rate}']
                    else:
                        partition.mounts = [f'{pct} {elapsed}']
                else:
                    partition.state = pct
                    slowdown =  partition.job.max_slowdown_ratio # temp?
                    stall =  partition.job.max_stall_secs      # temp
                    partition.mounts = [f'{elapsed} -{until} {rate} ÷{slowdown} 𝚫{Utils.ago_str(stall)}']

            if partition.parent and partition.parent in app.partitions and (
                    app.partitions[partition.parent].state == 'Lock'):
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

            # Show inline confirmation prompt if this is the partition being confirmed
            if app.confirmation.active and app.confirmation.partition_name == partition.name:
                # Build confirmation message
                if app.confirmation.confirm_type == 'wipe':
                    msg = f'⚠️  WIPE {partition.name} ({Utils.human(partition.size_bytes)})'
                else:  # verify
                    msg = f'⚠️  VERIFY {partition.name} ({Utils.human(partition.size_bytes)}) - writes marker'

                # Add mode-specific prompt
                if app.confirmation.mode == 'Y':
                    msg += " - Press 'Y' or ESC"
                elif app.confirmation.mode == 'y':
                    msg += " - Press 'y' or ESC"
                elif app.confirmation.mode == 'YES':
                    msg += f" - Type 'YES': {app.confirmation.input_buffer}_"
                elif app.confirmation.mode == 'yes':
                    msg += f" - Type 'yes': {app.confirmation.input_buffer}_"
                elif app.confirmation.mode == 'device':
                    msg += f" - Type '{partition.name}': {app.confirmation.input_buffer}_"

                # Position message at fixed column (reduced from 28 to 20)
                msg = ' ' * 20 + msg

                # Add confirmation message as DECOR (non-pickable)
                app.win.add_body(msg, attr=cs.color_pair(COLOR_DANGER) | cs.A_BOLD,
                               context=Context(genre='DECOR'))

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
                    app.confirmation.start('wipe', part.name, app.opts.confirmation)
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
                    app.confirmation.start('verify', part.name, app.opts.confirmation)
                    app.win.passthrough_mode = True
                else:
                    # Marked partition - proceed directly
                    # Clear any previous verify failure message when starting new verify
                    if hasattr(part, 'verify_failed_msg'):
                        delattr(part, 'verify_failed_msg')
                    part.job = WipeJob.start_verify_job(f'/dev/{part.name}',
                                                        part.size_bytes, opts=app.opts)
                    app.job_cnt += 1

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

    def lock_ACTION(self):
        """Handle 'l' key"""
        app = self.app
        ctx = app.win.get_picked_context()
        if ctx and hasattr(ctx, 'partition'):
            part = ctx.partition
            self.clear_hotswap_marker(part)
            app.set_state(part, 'Unlk' if part.state == 'Lock' else 'Lock')

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
    """History/log screen showing wipe history"""

    def draw_screen(self):
        """Draw the history screen"""
        app = self.app
        # spinner = self.get_spinner()

        app.win.set_pick_mode(False)

        # Add header
        app.win.add_header('WIPE HISTORY (newest first)', attr=cs.A_BOLD)
        app.win.add_header('    Press ESC to return', resume=True)

        # Read and display log file in reverse order
        log_path = Utils.get_log_path()
        if log_path.exists():
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                # Show in reverse order (newest first)
                for line in reversed(lines):
                    app.win.put_body(line.rstrip())
            except Exception as e:
                app.win.put_body(f'Error reading log: {e}')
        else:
            app.win.put_body('No wipe history found.')
            app.win.put_body('')
            app.win.put_body(f'Log file will be created at: {log_path}')


class ThemeScreen(DiskWipeScreen):
    """Theme preview screen showing all available themes with color examples"""
    prev_theme = ""

    def draw_screen(self):
        """Draw the theme screen with color examples for all themes"""
        app = self.app

        app.win.set_pick_mode(False)

        # Add header showing current theme

        app.win.add_header(f'COLOR THEME:  {app.opts.theme:^18}', attr=cs.A_BOLD)
        app.win.add_header('   Press [t] to cycle themes, ESC to return', resume=True)

        # Color purpose labels
        color_labels = [
            (COLOR_DANGER, 'DANGER', 'Destructive operations (wipe prompts)'),
            (COLOR_SUCCESS, 'SUCCESS', 'Completed operations'),
            (COLOR_WARNING, 'WARNING', 'Caution/stopped states'),
            (COLOR_INFO, 'INFO', 'Informational states'),
            (COLOR_EMPHASIS, 'EMPHASIS', 'Emphasized text'),
            (COLOR_ERROR, 'ERROR', 'Errors'),
            (COLOR_PROGRESS, 'PROGRESS', 'Progress indicators'),
            (COLOR_HOTSWAP, 'HOTSWAP', 'Newly inserted devices'),
        ]

        # Display color examples for current theme
        theme_info = THEMES[app.opts.theme]
        _ = theme_info.get('name', app.opts.theme)

        # Show color examples for this theme
        for color_id, label, description in color_labels:
            # Create line with colored block and description
            line = f'{label:12} ████████  {description}'
            attr = cs.color_pair(color_id)
            app.win.add_body(line, attr=attr)
            
    def spin_theme_ACTION(self):
        """ TBD """
        vals = list_themes()
        value = get_current_theme()
        idx = vals.index(value) if value in vals else -1
        value = vals[(idx+1) % len(vals)] # choose next
        set_theme(value)
        self.app.opts.theme = value
