"""
WipeJob class for handling disk/partition wiping operations

Orchestrates task sequences (write and verify operations).
"""
# pylint: disable=broad-exception-raised,broad-exception-caught
import os
import json
import time
import threading
import random
import traceback
import subprocess
import mmap
from types import SimpleNamespace

from .Utils import Utils
from .WipeTask import WipeTask
from .WriteTask import WriteTask, WriteZeroTask, WriteRandTask
from .VerifyTask import VerifyTask, VerifyZeroTask, VerifyRandTask


class WipeJob:
    """Handles disk/partition wiping operations with progress tracking

    Note: Constants and buffers are now defined in WipeTask base class.
    WipeJob uses WipeTask.BLOCK_SIZE, WipeTask.WRITE_SIZE, WipeTask.buffer, etc.
    """

    @staticmethod
    def _get_dirty_kb():
        """Read Dirty pages from /proc/meminfo (in KB)"""
        try:
            with open('/proc/meminfo', 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith('Dirty:'):
                        return int(line.split()[1])
        except Exception:
            pass
        return 0

    @staticmethod
    def _get_total_memory_mb():
        """Read total system memory from /proc/meminfo (in MB)"""
        try:
            with open('/proc/meminfo', 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith('MemTotal:'):
                        # Value is in KB, convert to MB
                        return int(line.split()[1]) // 1024
        except Exception:
            pass
        return 8192  # Default to 8GB if we can't read

    @staticmethod
    def _rebalance_buffer(buffer):
        """Rebalance byte distribution to be more uniform

        Takes random data and redistributes bytes to make the distribution
        more uniform, which helps with chi-squared verification while maintaining
        cryptographic randomness.
        """
        # Count byte frequencies
        byte_counts = [0] * 256
        for byte in buffer:
            byte_counts[byte] += 1

        # Calculate expected frequency (uniform distribution)
        expected = len(buffer) / 256

        # Find overused and underused bytes
        overused = []  # (byte_value, excess_count)
        underused = []  # (byte_value, deficit_count)

        for byte_val in range(256):
            count = byte_counts[byte_val]
            diff = count - expected
            if diff > expected * 0.05:  # More than 5% over expected
                overused.append((byte_val, int(diff * 0.7)))  # Replace 70% of excess
            elif diff < -expected * 0.05:  # More than 5% under expected
                underused.append((byte_val, int(-diff * 0.7)))  # Add to 70% of deficit

        if not overused or not underused:
            return buffer  # Already well-balanced

        # Create a mutable copy
        result = bytearray(buffer)

        # Build replacement plan
        replacements_made = 0
        target_replacements = min(sum(count for _, count in overused),
                                  sum(count for _, count in underused))

        # Randomly replace overused bytes with underused bytes
        underused_idx = 0
        for overused_byte, excess in overused:
            if replacements_made >= target_replacements:
                break

            # Find positions of this overused byte
            positions = [i for i, b in enumerate(result) if b == overused_byte]

            # Replace randomly selected positions
            replace_count = min(excess, target_replacements - replacements_made)
            positions_to_replace = random.sample(positions, min(replace_count, len(positions)))

            for pos in positions_to_replace:
                if underused_idx >= len(underused):
                    break
                underused_byte, deficit = underused[underused_idx]
                result[pos] = underused_byte
                replacements_made += 1

                # Move to next underused byte when we've used up this one's deficit
                if replacements_made % max(1, deficit) == 0:
                    underused_idx += 1

        return bytes(result)

    def __init__(self, device_path, total_size, opts=None, tasks=None):
        """Initialize WipeJob as a task orchestrator

        Args:
            device_path: Path to device (e.g., '/dev/sda1')
            total_size: Total size in bytes
            opts: Options namespace
            tasks: List of WipeTask instances to execute sequentially (if None, legacy mode)
        """
        self.opts = opts if opts else SimpleNamespace(dry_run=False)
        self.device_path = device_path
        self.total_size = total_size
        self.do_abort = False
        self.thread = None

        # Task orchestration (new)
        self.tasks = tasks if tasks else []  # List of WipeTask instances
        self.current_task = None  # Currently executing task
        self.current_task_index = 0  # Index of current task

        # Legacy attributes (kept for backwards compatibility)
        self.start_mono = time.monotonic()  # Track the start time
        self.total_written = 0  # Will be set by tasks
        self.resume_from = 0  # Track resume offset
        self.resume_mode = None  # Original mode if resuming (overrides opts.wipe_mode)
        self.wr_hists = []  # list of (mono, written)
        self.done = False
        self.exception = None  # in case of issues

        # Multi-pass tracking (legacy, now handled by task sequence)
        self.passes = getattr(opts, 'passes', 1)  # Total number of passes to perform
        self.current_pass = 0  # Current pass number (0-indexed)

        # Verification tracking (proxied from verify tasks)
        self.verify_phase = False  # True when verifying
        self.verify_start_mono = None  # Start time of verify phase
        self.verify_progress = 0  # Bytes verified so far
        self.verify_pct = 0  # Percentage of disk being verified (e.g., 2 for 2%)
        self.verify_result = None  # "zeroed", "random", "not-wiped", "mixed"

        self.expected_pattern = None  # "zero" or "random" for what we wrote
        self.is_verify_only = False  # True if this is a standalone verify job

        # Periodic marker updates for crash recovery
        self.last_marker_update_mono = time.monotonic() - 25  # Last time we wrote progress marker
        self.marker_update_interval = 30  # Update marker every 30 seconds

        ## SLOWDOWN / STALL DETECTION/ABORT FEATURE (proxied from tasks)
        ##
        self.slowdown_stop = getattr(opts, 'slowdown_stop', 16)
        self.stall_timeout = getattr(opts, 'stall_timeout', 60)
        self.max_slowdown_ratio = 0
        self.max_stall_secs = 0
            # Initialize tracking variables
        self.baseline_speed = None  # Bytes per second
        self.baseline_end_mono = None  # When baseline measurement ended
            # Stall tracking
        self.last_progress_mono = time.monotonic()  # Last time we made progress
        self.last_progress_written = 0  # Bytes written at last progress check
            # For periodic slowdown checks (every 10 seconds)
        self.last_slowdown_check = 0
            # Initialize write history for speed calculation
        self.wr_hists.append(SimpleNamespace(mono=self.start_mono, written=0))

        # ERROR ABORT FEATURE (proxied from write tasks)
        self.max_consecutive_errors = 3 # a control
        self.max_total_errors = 100 # a control
        self.reopen_on_error = True # a control
        self.reopen_count = 0 # cumulative (info only)
        self.total_errors = 0 # cumulative



    def run_tasks(self):
        """Execute task sequence and update WipeJob state from tasks

        This is the main orchestration method that runs each task in sequence.
        It proxies task state to WipeJob attributes for backwards compatibility.
        """
        # Start abort flag sync thread
        stop_sync = [False]

        def sync_abort_flag():
            """Continuously sync WipeJob.do_abort to current task"""
            while not stop_sync[0]:
                if self.current_task and not self.current_task.do_abort:
                    self.current_task.do_abort = self.do_abort
                time.sleep(0.1)  # Check every 100ms

        sync_thread = threading.Thread(target=sync_abort_flag, daemon=True)
        sync_thread.start()

        try:
            for i, task in enumerate(self.tasks):
                # Skip already-completed tasks (from resume logic)
                if task.done:
                    continue

                if self.do_abort:
                    break

                self.current_task = task
                self.current_task_index = i

                # Run the task
                task.run_task()

                # Check for task errors
                if task.exception:
                    self.exception = task.exception
                    break

                # Check if task was aborted (sync abort state)
                if task.do_abort and not self.do_abort:
                    self.do_abort = True
                    break

                # Proxy task state back to WipeJob for compatibility
                # Write tasks
                if isinstance(task, WriteTask):
                    self.total_written = task.total_written
                    self.max_slowdown_ratio = max(self.max_slowdown_ratio, task.max_slowdown_ratio)
                    self.max_stall_secs = max(self.max_stall_secs, task.max_stall_secs)
                    self.total_errors = task.total_errors
                    self.reopen_count = task.reopen_count
                    # Set expected pattern from write task
                    if isinstance(task, WriteZeroTask):
                        self.expected_pattern = "zeroed"
                    elif isinstance(task, WriteRandTask):
                        self.expected_pattern = "random"

                # Verify tasks
                elif isinstance(task, VerifyTask):
                    self.verify_phase = True
                    if self.verify_start_mono is None:
                        self.verify_start_mono = task.start_mono
                    self.verify_progress = task.total_written  # VerifyTask uses total_written for progress
                    self.verify_pct = task.verify_pct
                    # Extract verify result from task summary
                    summary = task.get_summary_dict()
                    self.verify_result = summary.get('result', None)

        finally:
            # Stop abort sync thread
            stop_sync[0] = True
            # Always mark as done when tasks complete
            self.done = True
            self.current_task = None

    @staticmethod
    def _get_pass_pattern_static(pass_number, total_passes, desired_mode):
        """Static version of get_pass_pattern for use in start_job()

        Determine what pattern to write for a given pass.

        Args:
            pass_number: 0-indexed pass number
            total_passes: Total number of passes
            desired_mode: 'Rand' or 'Zero' - the final desired pattern

        Returns:
            bool: True for random, False for zeros
        """
        if total_passes == 1:
            # Single pass: just write desired pattern
            return desired_mode == 'Rand'

        # Multi-pass: alternate patterns, ending on desired
        # Final pass is always desired pattern
        if pass_number == total_passes - 1:
            return desired_mode == 'Rand'

        # Earlier passes: alternate, starting with opposite
        if desired_mode == 'Rand':
            # Even passes (0, 2, ...) = Zero, odd (1, 3, ...) = Rand
            return pass_number % 2 == 1
        else:
            # Even passes (0, 2, ...) = Rand, odd (1, 3, ...) = Zero
            return pass_number % 2 == 0

    @staticmethod
    def start_job(device_path, total_size, opts):
        """Start a wipe job in a background thread

        If an existing marker with stopped state is found, resumes from that offset.
        Uses current passes setting to determine target bytes (may differ from original).

        Smart resume logic for multi-pass:
        - If resuming in the final pass, checks pattern on disk
        - If pattern matches expected for final pass, resumes from current position
        - If pattern doesn't match, restarts final pass from beginning
        This prevents mixed patterns (e.g., random then zeros) in the final result.
        """
        resume_from = 0
        resume_mode = None

        # Check for existing marker to resume from
        device_name = device_path.replace('/dev/', '')
        existing_marker = WipeJob.read_marker_buffer(device_name)
        if existing_marker and existing_marker.scrubbed_bytes > 0:
            scrubbed = existing_marker.scrubbed_bytes
            resume_mode = existing_marker.mode  # Use original mode ('Rand' or 'Zero')
            passes = getattr(opts, 'passes', 1)

            # Check if the marker indicates a completed wipe
            # Old markers without 'passes' field: assume 1 pass
            marker_passes = getattr(existing_marker, 'passes', 1)
            marker_target = total_size * marker_passes

            if scrubbed >= marker_target:
                # Wipe was completed with marker_passes - user wants to re-wipe
                # Start fresh from 0
                resume_from = 0
                resume_mode = None
            else:
                   # Partial/stopped wipe - resume from where it left off
                resume_from = scrubbed
                # Ensure we don't resume in the marker area
                if resume_from < WipeTask.MARKER_SIZE:
                    resume_from = WipeTask.MARKER_SIZE
                # Also ensure not past the end (sanity check)
                if resume_from > total_size * getattr(opts, 'passes', 1):
                    resume_from = 0  # Start over if marker corrupted

                # Partial/stopped wipe - resume from where it left off
                # Smart resume: check if we're in the final pass
                current_pass = scrubbed // total_size
                last_pass_num = passes - 1

                if current_pass >= last_pass_num:
                    # Resuming in final pass - check pattern on disk matches expected
                    # Create temporary job to use get_pass_pattern method
                    temp_job = WipeJob(device_path, total_size, opts)
                    temp_job.passes = passes
                    expected_is_random = temp_job.get_pass_pattern(last_pass_num, resume_mode)

                    # Detect actual pattern on disk
                    actual_is_random = WipeJob.detect_pattern_on_disk(device_path)

                    if expected_is_random != actual_is_random:
                        # Pattern mismatch - restart final pass from beginning
                        resume_from = last_pass_num * total_size
                    else:
                        # Pattern matches - resume from current position
                        resume_from = scrubbed

        # Build task sequence
        tasks = []
        mode = getattr(opts, 'wipe_mode', 'Rand')
        base_mode = mode.replace('+V', '')  # Remove verification suffix
        auto_verify = '+V' in mode
        passes = getattr(opts, 'passes', 1)

        # Use resume_mode if resuming, otherwise use current mode
        desired_mode = resume_mode if resume_mode else base_mode

        # Build write task sequence (alternating patterns for multi-pass)
        for pass_num in range(passes):
            # Determine pattern for this pass
            is_random = WipeJob._get_pass_pattern_static(pass_num, passes, desired_mode)

            # Calculate pass offset and resume point
            pass_start = pass_num * total_size
            pass_resume = 0
            if resume_from > pass_start:
                # Resume within this pass
                pass_resume = resume_from - pass_start

            # Create appropriate write task
            if is_random:
                task = WriteRandTask(device_path, total_size, opts,
                                    resume_from=pass_resume, pass_number=pass_num)
            else:
                task = WriteZeroTask(device_path, total_size, opts,
                                    resume_from=pass_resume, pass_number=pass_num)
            tasks.append(task)

            # If we haven't reached the resume point yet, skip this task
            if resume_from >= (pass_num + 1) * total_size:
                # This pass is already complete, mark task as done
                task.done = True
                task.total_written = total_size

        # Add auto-verification task if requested
        if auto_verify:
            verify_pct = getattr(opts, 'verify_pct', 2)
            if desired_mode == 'Rand':
                task = VerifyRandTask(device_path, total_size, opts, verify_pct=verify_pct)
            else:
                task = VerifyZeroTask(device_path, total_size, opts, verify_pct=verify_pct)
            tasks.append(task)

        # Create WipeJob with task sequence
        job = WipeJob(device_path=device_path, total_size=total_size, opts=opts, tasks=tasks)
        job.resume_from = resume_from
        job.resume_mode = resume_mode
        job.thread = threading.Thread(target=job.run_tasks)
        job.thread.start()
        return job

    @staticmethod
    def start_verify_job(device_path, total_size, opts, expected_pattern=None):
        """Start a verification-only job in a background thread

        Args:
            device_path: Path to device (e.g., '/dev/sda1')
            total_size: Total size in bytes
            opts: Options namespace with verify_pct
            expected_pattern: "zeroed", "random", or None (auto-detect)
        """
        # Read existing marker to determine expected pattern if not specified
        device_name = os.path.basename(device_path)
        existing_marker = WipeJob.read_marker_buffer(device_name)
        if existing_marker and expected_pattern is None:
            expected_pattern = "random" if existing_marker.mode == 'Rand' else "zeroed"

        verify_pct = getattr(opts, 'verify_pct', 0)
        if verify_pct == 0:
            verify_pct = 2  # Default to 2% if not set

        # Create verify task
        tasks = []
        if expected_pattern == "random":
            task = VerifyRandTask(device_path, total_size, opts, verify_pct=verify_pct)
        else:
            task = VerifyZeroTask(device_path, total_size, opts, verify_pct=verify_pct)
        tasks.append(task)

        # Create WipeJob with verify task
        job = WipeJob(device_path=device_path, total_size=total_size, opts=opts, tasks=tasks)
        job.is_verify_only = True
        job.expected_pattern = expected_pattern
        job.verify_phase = True

        def verify_runner():
            try:
                # Run the verify task
                job.run_tasks()

                # Write marker with verification status
                if existing_marker:
                    is_random = existing_marker.mode == 'Rand'
                    job._write_marker_with_verify_status(is_random)
            except Exception:
                job.exception = traceback.format_exc()
            finally:
                # ALWAYS ensure job is marked as done
                if not job.done:
                    job.done = True

        job.thread = threading.Thread(target=verify_runner)
        job.thread.start()
        return job

    def _check_for_stall(self, current_monotonic):
        """Check for stall (no progress) - called frequently"""
        if self.stall_timeout <= 0:
            return False

        time_since_progress = current_monotonic - self.last_progress_mono
        self.max_stall_secs = max(time_since_progress, self.max_stall_secs)
        if time_since_progress >= self.stall_timeout:
            self.do_abort = True
            self.exception = f"Stall detected: No progress for {time_since_progress:.1f} seconds"
            return True

        return False

    def _check_for_slowdown(self, current_monotonic):
        """Check for slowdown - called every 10 seconds"""
        if self.slowdown_stop <= 0 or self.baseline_speed is None or self.baseline_speed <= 0:
            return False

        # Calculate current speed over last 30 seconds
        floor = current_monotonic - 30
        recent_history = [h for h in self.wr_hists if h.mono >= floor]

        if len(recent_history) >= 2:
            recent_start = recent_history[0]
            recent_written = self.total_written - recent_start.written
            recent_elapsed = current_monotonic - recent_start.mono

            if recent_elapsed > 1.0:
                current_speed = recent_written / recent_elapsed
                self.baseline_speed = max(self.baseline_speed, current_speed)
                slowdown_ratio = self.baseline_speed / max(current_speed, 1)
                slowdown_ratio = int(round(slowdown_ratio, 0))
                self.max_slowdown_ratio = max(self.max_slowdown_ratio, slowdown_ratio)

                if slowdown_ratio > self.slowdown_stop:
                    self.do_abort = True
                    self.exception = (f"Slowdown abort: ({Utils.human(current_speed)}B/s)"
                                     f" is 1/{slowdown_ratio} baseline")
                    return True

        return False

    def _update_baseline_if_needed(self, current_monotonic):
        """Update baseline speed measurement if still in first 60 seconds"""
        if self.baseline_speed is not None:
            return  # Baseline already established

        if (current_monotonic - self.start_mono) >= 60:
            total_written_60s = self.total_written - self.resume_from
            elapsed_60s = current_monotonic - self.start_mono
            if elapsed_60s > 0:
                self.baseline_speed = total_written_60s / elapsed_60s
                self.baseline_end_mono = current_monotonic
                self.last_slowdown_check = current_monotonic  # Start slowdown checking



    def get_status_str(self):
        """Get human-readable status string"""
        elapsed_time = time.monotonic() - self.start_mono
        write_rate = self.total_written / elapsed_time if elapsed_time > 0 else 0
        percent_complete = (self.total_written / self.total_size) * 100
        return (f"Write rate: {write_rate / (1024 * 1024):.2f} MB/s, "
                f"Completed: {percent_complete:.2f}%")

    def get_status(self):
        """Get status tuple: (elapsed, percent, rate, eta)

        Returns stats for current phase only:
        - Write phase (0-100%): elapsed/rate/eta for writing
        - Flushing phase: 100% FLUSH while kernel syncs to device
        - Verify phase (v0-v100%): elapsed/rate/eta for verification only
        """
        # NEW: Proxy to current task if using task-based architecture
        if self.current_task is not None:
            return self.current_task.get_status()

        # LEGACY: Original implementation for backwards compatibility
        pct_str, rate_str, when_str = '', '', ''
        mono = time.monotonic()

        if self.verify_phase:
            # Verification phase: v0% to v100%
            if self.verify_start_mono is None:
                self.verify_start_mono = mono

            elapsed_time = mono - self.verify_start_mono
            progress = self.verify_progress

            # Calculate total bytes to verify (verify_pct% of total_size)
            if self.verify_pct > 0:
                total_to_verify = self.total_size * self.verify_pct / 100
            else:
                total_to_verify = self.total_size

            # Calculate verification percentage (0-100)
            pct = int((progress / total_to_verify) * 100) if total_to_verify > 0 else 0
            pct_str = f'v{pct}%'

            if self.do_abort:
                pct_str = 'STOP'

            # Track verification progress for rate calculation
            self.wr_hists.append(SimpleNamespace(mono=mono, written=progress))
            floor = mono - 30
            while len(self.wr_hists) >= 3 and self.wr_hists[1].mono >= floor:
                del self.wr_hists[0]
            delta_mono = mono - self.wr_hists[0].mono
            physical_rate = (progress - self.wr_hists[0].written) / delta_mono if delta_mono > 1.0 else 0
            # Scale rate to show "effective" verification rate (as if verifying 100% of disk)
            effective_rate = physical_rate * (100 / self.verify_pct) if self.verify_pct > 0 else physical_rate
            rate_str = f'{Utils.human(int(round(effective_rate, 0)))}/s'

            if physical_rate > 0:
                remaining = total_to_verify - progress
                when = int(round(remaining / physical_rate))
                when_str = Utils.ago_str(when)
            else:
                when_str = '0'

            return Utils.ago_str(int(round(elapsed_time))), pct_str, rate_str, when_str
        else:
            # Write phase: 0-100% (across all passes)
            written = self.total_written
            elapsed_time = mono - self.start_mono

            # Calculate progress across all passes
            # total_written represents cumulative bytes across all passes
            total_work = self.total_size * self.passes
            pct = (self.total_written / total_work) * 100 if total_work > 0 else 0
            # Cap at 100% (can exceed if passes changed during resume)
            pct = min(pct, 100)
            pct_str = f'{int(round(pct))}%'
            if self.do_abort:
                pct_str = 'STOP'

            # Calculate rate using sliding window to avoid RAM buffering inflation
            # Track write progress history
            self.wr_hists.append(SimpleNamespace(mono=mono, written=written))
            floor = mono - 30  # 30 second window
            while len(self.wr_hists) >= 3 and self.wr_hists[1].mono >= floor:
                del self.wr_hists[0]

            # Calculate rate from sliding window
            delta_mono = mono - self.wr_hists[0].mono
            rate = (written - self.wr_hists[0].written) / delta_mono if delta_mono > 1.0 else 0

            rate_str = f'{Utils.human(int(round(rate, 0)))}/s'

            if rate > 0:
                # ETA based on total remaining work across all passes
                remaining_work = total_work - self.total_written
                when = int(round(remaining_work / rate))
                when_str = Utils.ago_str(when)
            else:
                when_str = '0'

            return Utils.ago_str(int(round(elapsed_time))), pct_str, rate_str, when_str

    def get_plan_dict(self, mode=None):
        """Generate plan dictionary for structured logging

        Args:
            mode: Optional mode override (e.g., 'Rand', 'Zero', 'Rand+V')
                  If None, uses self.opts.wipe_mode

        Returns:
            dict: Plan section with operation, steps, mode, verify settings, passes
        """
        if mode is None:
            mode = getattr(self.opts, 'wipe_mode', 'Unknown')

        # Build steps list
        steps = []

        # Extract base mode (remove +V suffix)
        base_mode = mode.replace('+V', '')
        verify_in_mode = '+V' in mode

        # Add wipe steps (one per pass)
        for pass_num in range(self.passes):
            if self.passes > 1:
                steps.append(f"wipe {base_mode} {self.device_path} (pass {pass_num + 1}/{self.passes})")
            else:
                steps.append(f"wipe {base_mode} {self.device_path}")

        # Add verify step if enabled
        verify_pct = getattr(self.opts, 'verify_pct', 0)
        if verify_in_mode or verify_pct > 0:
            if verify_pct > 0 and verify_pct < 100:
                steps.append(f"verify {base_mode} ({verify_pct}% sample)")
            else:
                steps.append(f"verify {base_mode}")

        return {
            "operation": "verify" if self.is_verify_only else "wipe",
            "steps": steps,
            "mode": base_mode,
            "verify_enabled": verify_in_mode or verify_pct > 0,
            "verify_pct": verify_pct,
            "passes": self.passes,
            "slowdown_stop_threshold": self.slowdown_stop,
            "stall_timeout_threshold": self.stall_timeout,
        }

    def get_summary_dict(self):
        """Generate complete summary dictionary for structured logging

        Returns:
            dict: Summary with top-level aggregates and per-step details
        """
        # NEW: Aggregate task summaries if using task-based architecture
        if self.tasks:
            mono = time.monotonic()
            total_elapsed = mono - self.start_mono

            # Aggregate all task summaries
            steps = []
            total_errors = 0
            for task in self.tasks:
                task_summary = task.get_summary_dict()
                steps.append(task_summary)
                total_errors += task_summary.get('errors', 0)

            # Build top-level summary
            summary = {
                "result": "stopped" if self.do_abort else "completed",
                "total_elapsed": Utils.ago_str(int(total_elapsed)),
                "total_errors": total_errors,
                "pct_complete": 100.0 if self.done and not self.do_abort else 0.0,
                "resumed_from_bytes": self.resume_from,
                "steps": steps,
            }
            return summary

        # LEGACY: Original implementation for backwards compatibility
        mono = time.monotonic()
        write_elapsed = mono - self.start_mono

        # Calculate write rates
        write_rate_bps = self.total_written / write_elapsed if write_elapsed > 0 else 0

        # Calculate completion percentage
        total_work = self.total_size * self.passes
        pct_complete = min(100, (self.total_written / total_work) * 100 if total_work > 0 else 0)

        # Build wipe step
        mode = getattr(self.opts, 'wipe_mode', 'Unknown').replace('+V', '')
        wipe_step = {
            "step": f"wipe {mode} {self.device_path}",
            "elapsed": Utils.ago_str(int(write_elapsed)),
            "rate": f"{Utils.human(int(write_rate_bps))}/s",
            "bytes_written": self.total_written,
            "bytes_total": total_work,
            "passes_total": self.passes,
            "passes_completed": min(self.total_written // self.total_size, self.passes),
            "current_pass": self.current_pass,
            "peak_write_rate": f"{Utils.human(int(self.baseline_speed))}/s" if self.baseline_speed else None,
            "worst_stall": Utils.ago_str(int(self.max_stall_secs)),
            "worst_slowdown_ratio": round(self.max_slowdown_ratio, 1),
            "errors": self.total_errors,
            "reopen_count": self.reopen_count,
        }

        # Build steps array
        steps = [wipe_step]

        # Add verification step if verify was done
        total_elapsed = write_elapsed
        if self.verify_start_mono:
            verify_elapsed = mono - self.verify_start_mono
            total_elapsed = write_elapsed + verify_elapsed
            verify_rate_bps = self.verify_progress / verify_elapsed if verify_elapsed > 0 else 0

            # Extract verify detail from verify_result if it contains extra info
            verify_detail = None
            if self.verify_result and '(' in str(self.verify_result):
                # Extract detail from results like "not-wiped (non-zero at 22K)"
                verify_detail = str(self.verify_result).split('(')[1].rstrip(')')

            verify_pct = getattr(self.opts, 'verify_pct', 0)
            verify_label = f"verify {mode}"
            if verify_pct > 0 and verify_pct < 100:
                verify_label += f" ({verify_pct}% sample)"

            verify_step = {
                "step": verify_label,
                "elapsed": Utils.ago_str(int(verify_elapsed)),
                "rate": f"{Utils.human(int(verify_rate_bps))}/s",
                "bytes_checked": self.verify_progress,
                "result": self.verify_result,
            }
            if verify_detail:
                verify_step["verify_detail"] = verify_detail

            steps.append(verify_step)

        # Build top-level summary
        summary = {
            "result": "stopped" if self.do_abort else "completed",
            "total_elapsed": Utils.ago_str(int(total_elapsed)),
            "total_errors": self.total_errors,
            "pct_complete": round(pct_complete, 1),
            "resumed_from_bytes": self.resume_from,
            "steps": steps,
        }

        return summary

    def prep_marker_buffer(self, is_random, verify_status=None):
        """Get the 1st 16KB to write:
        - 15K zeros
        - JSON status + zero fill to 1KB

        Marker format (JSON):
            - unixtime: Unix timestamp when marker was written
            - scrubbed_bytes: Total bytes written (including all passes)
            - size_bytes: Total device size in bytes
            - passes: Number of passes intended/completed
            - mode: 'Rand' or 'Zero' (final desired pattern)
            - verify_status: 'pass', 'fail', or omitted (not verified)

        Args:
            is_random: bool, whether random data was written
            verify_status: str, "pass", "fail", or None (not verified)
        """
        data = {"unixtime": int(time.time()),
                "scrubbed_bytes": self.total_written,
                "size_bytes": self.total_size,
                "passes": self.passes,
                "mode": 'Rand' if is_random else 'Zero'
                }
        if verify_status is not None:
            data["verify_status"] = verify_status
        json_data = json.dumps(data).encode('utf-8')
        buffer = bytearray(WipeTask.MARKER_SIZE)  # Only 16KB, not 1MB
        buffer[:WipeTask.STATE_OFFSET] = b'\x00' * WipeTask.STATE_OFFSET
        buffer[WipeTask.STATE_OFFSET:WipeTask.STATE_OFFSET + len(json_data)] = json_data
        remaining_size = WipeTask.MARKER_SIZE - (WipeTask.STATE_OFFSET + len(json_data))
        buffer[WipeTask.STATE_OFFSET + len(json_data):] = b'\x00' * remaining_size
        return buffer

    def get_pass_pattern(self, pass_number, desired_mode):
        """Determine what pattern to write for a given pass

        For multi-pass (>1), alternates patterns ending on desired:
        - 4-pass Rand: Zero, Rand, Zero, Rand
        - 4-pass Zero: Rand, Zero, Rand, Zero
        - 2-pass Rand: Zero, Rand
        - 2-pass Zero: Rand, Zero

        Args:
            pass_number: 0-indexed pass number
            desired_mode: 'Rand' or 'Zero' - the final desired pattern

        Returns:
            bool: True for random, False for zeros
        """
        if self.passes == 1:
            # Single pass: just write desired pattern
            return desired_mode == 'Rand'

        # Multi-pass: alternate patterns, ending on desired
        # Final pass is always desired pattern
        if pass_number == self.passes - 1:
            return desired_mode == 'Rand'

        # Earlier passes: alternate, starting with opposite
        # If desired is Rand: pass 0=Zero, 1=Rand, 2=Zero, 3=Rand
        # If desired is Zero: pass 0=Rand, 1=Zero, 2=Rand, 3=Zero
        if desired_mode == 'Rand':
            # Even passes (0, 2, ...) = Zero, odd (1, 3, ...) = Rand
            return pass_number % 2 == 1
        else:
            # Even passes (0, 2, ...) = Rand, odd (1, 3, ...) = Zero
            return pass_number % 2 == 0

    def maybe_update_marker(self, is_random):
        """Periodically update marker to enable crash recovery

        Updates marker every marker_update_interval seconds (default 30s).
        This allows resume to work even after crashes, power loss, or kill -9.

        Args:
            is_random: bool, whether random data is being written

        Returns:
            None
        """
        now_mono = time.monotonic()
        if now_mono - self.last_marker_update_mono < self.marker_update_interval:
            return  # Not time yet

        # Marker writes use separate file handle (buffered I/O, not O_DIRECT)
        # because marker buffer is not aligned
        try:
            with open(self.device_path, 'r+b') as marker_file:
                marker_file.seek(0)
                marker_file.write(self.prep_marker_buffer(is_random))
            self.last_marker_update_mono = now_mono
        except Exception:
            # If marker update fails, just continue - we'll try again in 30s
            pass

    @staticmethod
    def detect_pattern_on_disk(device_path, sample_size=16*1024):
        """Read a sample from disk after header to detect zeros vs random

        Reads sample_size bytes starting at WRITE_SIZE (after the marker area).
        Used for smart resume to determine if we're in the middle of a Zero or Rand pass.

        Args:
            device_path: Path to device (e.g., '/dev/sda1')
            sample_size: Number of bytes to sample (default 16KB)

        Returns:
            bool: True if random pattern detected, False if zeros detected
        """
        try:
            with open(device_path, 'rb') as device:
                # Skip past the marker area (first 16KB)
                device.seek(WipeTask.WRITE_SIZE)
                data = device.read(sample_size)

                if not data:
                    return False  # Can't read, assume zeros

                # Check if all zeros
                non_zero_count = sum(1 for byte in data if byte != 0)
                # If less than 1% non-zero bytes, consider it zeros
                if non_zero_count < len(data) * 0.01:
                    return False  # Zeros detected

                # Otherwise, assume random
                return True
        except Exception:
            return False  # Error reading, assume zeros

    def _get_device_major_minor(self):
        """Get major:minor device numbers for the device

        Returns:
            str: "major:minor" or None if unable to determine
        """
        try:
            stat_info = os.stat(self.device_path)
            major = os.major(stat_info.st_rdev)
            minor = os.minor(stat_info.st_rdev)
            return f"{major}:{minor}"
        except Exception:
            return None

    def _setup_ionice(self):
        """Setup I/O priority to best-effort class, lowest priority"""
        try:
            # Class 2 = best-effort, priority 7 = lowest (0 is highest, 7 is lowest)
            subprocess.run(["ionice", "-c", "2", "-n", "7", "-p", str(os.getpid())],
                          capture_output=True, check=False)
        except Exception:
            pass

    @staticmethod
    def read_marker_buffer(device_name):
        """Open the device and read the first 16 KB"""
        try:
            with open(f'/dev/{device_name}', 'rb') as device:
                device.seek(0)
                buffer = device.read(WipeTask.MARKER_SIZE)
        except Exception:
            return None  # cannot find info

        if buffer[:WipeTask.STATE_OFFSET] != b'\x00' * (WipeTask.STATE_OFFSET):
            return None  # First 15 KB are not zeros

        # Extract JSON data from the next 1 KB Strip trailing zeros
        json_data_bytes = buffer[WipeTask.STATE_OFFSET:WipeTask.MARKER_SIZE].rstrip(b'\x00')

        if not json_data_bytes:
            return None  # No JSON data found

        # Deserialize the JSON data
        try:
            data = json.loads(json_data_bytes.decode('utf-8'))
        except (json.JSONDecodeError, Exception):
            return None  # Invalid JSON data!

        rv = {}
        for key, value in data.items():
            if key in ('unixtime', 'scrubbed_bytes', 'size_bytes', 'passes') and isinstance(value, int):
                rv[key] = value
            elif key in ('mode', 'verify_status') and isinstance(value, str):
                rv[key] = value
            else:
                return None  # bogus data
        # Old markers: 4 fields (no passes, no verify_status)
        # New markers: 5 fields minimum (with passes), 6 with verify_status
        if len(rv) < 4 or len(rv) > 6:
            return None  # bogus data
        return SimpleNamespace(**rv)

    def write_partition(self):
        """Writes random chunks to a device and updates the progress status.

        Performs multiple passes if self.passes > 1 with alternating patterns:
        - Multi-pass Rand: Zero, Rand, Zero, Rand (ends on Rand)
        - Multi-pass Zero: Rand, Zero, Rand, Zero (ends on Zero)
        If verify_pct > 0, automatically starts verification after write completes.
        Supports resuming from a previous stopped wipe with current passes setting.
        """
        # Use resume_mode if set (from existing marker), otherwise current mode
        mode_to_use = self.resume_mode if self.resume_mode else self.opts.wipe_mode.replace('+V', '')
        desired_mode = mode_to_use  # 'Rand' or 'Zero' - the final desired pattern
        self.expected_pattern = "random" if desired_mode == 'Rand' else "zeroed"

        # Calculate target bytes based on current passes setting
        target_bytes = self.passes * self.total_size

        # If already at or beyond target (user reduced passes), mark as done
        if self.total_written >= target_bytes:
            self.done = True
            return

        try:
            # Set low I/O priority to be nice to other system processes
            self._setup_ionice()

            # Open device with O_DIRECT for unbuffered I/O (bypasses page cache)
            # O_DIRECT gives maximum performance with zero dirty pages
            if not self.opts.dry_run:
                fd = os.open(self.device_path, os.O_WRONLY | os.O_DIRECT)
            else:
                fd = None

            try:
                # Continue writing until we reach target_bytes
                while self.total_written < target_bytes and not self.do_abort:
                    # Calculate current pass and offset within pass
                    self.current_pass = self.total_written // self.total_size
                    offset_in_pass = self.total_written % self.total_size

                    # SKIP MARKER AREA - don't overwrite it!
                    if offset_in_pass < WipeTask.MARKER_SIZE:
                        self.total_written += WipeTask.MARKER_SIZE - offset_in_pass
                        offset_in_pass = WipeTask.MARKER_SIZE

                    # Determine pattern for this pass (alternating for multi-pass)
                    is_random_pass = self.get_pass_pattern(self.current_pass, desired_mode)

                    # Seek to current position (O_DIRECT requires block-aligned seeks)
                    if not self.opts.dry_run:
                        os.lseek(fd, offset_in_pass, os.SEEK_SET)

                    # Write until end of current pass or target_bytes, whichever comes first
                    pass_remaining = self.total_size - offset_in_pass
                    total_remaining = target_bytes - self.total_written
                    bytes_to_write_this_pass = min(pass_remaining, total_remaining)

                    pass_bytes_written = 0

                    while pass_bytes_written < bytes_to_write_this_pass and not self.do_abort:
                        current_mono = time.monotonic()

                        # Update baseline if needed (first 60 seconds)
                        self._update_baseline_if_needed(current_mono)

                        # Check for stall (frequently)
                        if self._check_for_stall(current_mono):
                            break

                        # Check for slowdown (every 10 seconds)
                        if self.baseline_speed is not None:
                            time_since_last_check = current_mono - self.last_slowdown_check
                            if time_since_last_check >= 10:
                                if self._check_for_slowdown(current_mono):
                                    break
                                self.last_slowdown_check = current_mono

                        # Update progress tracking
                        if self.total_written > self.last_progress_written:
                            self.last_progress_mono = current_mono


                        # Calculate chunk size (must be block-aligned for O_DIRECT)
                        remaining = bytes_to_write_this_pass - pass_bytes_written
                        chunk_size = min(WipeTask.WRITE_SIZE, remaining)
                        # Round down to block boundary
                        chunk_size = (chunk_size // WipeTask.BLOCK_SIZE) * WipeTask.BLOCK_SIZE
                        if chunk_size == 0:
                            break

                        # Select buffer based on pass type
                        if is_random_pass:
                            # Use slice of random buffer (still aligned via memoryview)
                            chunk = WipeTask.buffer[:chunk_size]
                        else:
                            # Use zero buffer
                            chunk = WipeTask.zero_buffer[:chunk_size]

                        if self.opts.dry_run:
                            bytes_written = chunk_size
                            time.sleep(0.001)
                        else:
                            try:
                                # Write with O_DIRECT (bypasses page cache)
                                bytes_written, fd = self.safe_write(fd, chunk)
                            except Exception as e:
                                # Save exception for debugging
                                self.exception = str(e)
                                self.do_abort = True
                                bytes_written = 0

                        self.total_written += bytes_written
                        pass_bytes_written += bytes_written

                        # Periodically update marker for crash recovery (every 30s)
                        # Note: marker writes use separate buffered file handle
                        if not self.opts.dry_run and self.total_written > WipeTask.MARKER_SIZE:
                            marker_is_random = (desired_mode == 'Rand')
                            self.maybe_update_marker(marker_is_random)

                        # Check for errors or incomplete writes
                        if bytes_written < chunk_size:
                            break

                # O_DIRECT has no dirty pages - close is instant
            finally:
                # Close device file descriptor
                if fd is not None:
                    os.close(fd)

            # Write final marker buffer at beginning after ALL passes complete
            # Skip marker write on abort to avoid blocking on problematic devices
            # Use separate buffered file handle (marker is not O_DIRECT aligned)
            if not self.opts.dry_run and self.total_written > 0 and not self.do_abort:
                try:
                    final_is_random = (desired_mode == 'Rand')
                    with open(self.device_path, 'r+b') as marker_file:
                        marker_file.seek(0)
                        marker_file.write(self.prep_marker_buffer(final_is_random))
                except Exception:
                    pass  # Marker write failure shouldn't fail the whole job

            # Auto-start verification if enabled and write completed successfully
            verify_pct = getattr(self.opts, 'verify_pct', 0)
            auto_verify = getattr(self.opts, 'wipe_mode', "").endswith('+V')
            if auto_verify and verify_pct > 0 and not self.do_abort and not self.exception:
                self.verify_partition(verify_pct)
                # Write marker with verification status after verification completes
                # Use desired_mode to determine if random or zero
                is_random = (desired_mode == 'Rand')
                self._write_marker_with_verify_status(is_random)
            else:
                self.done = True
        except Exception:
            self.exception = traceback.format_exc()
        finally:
            # ALWAYS ensure job is marked as done, even if exception or early return
            if not self.done:
                self.done = True

    def safe_write(self, fd, chunk):
        """Safe write with error recovery.

        Returns:
            tuple: (bytes_written, fd) - bytes_written is either:
                   - Actual bytes written (success)
                   - len(chunk) (failed but non-fatal - skip entire chunk)
                   fd might be new if reopened

        Raises:
            Exception: If should abort (too many consecutive errors)
        """
        consecutive_errors = 0
        while True:  # Keep trying until success, skip, or abort
            try:
                bytes_written = os.write(fd, chunk)
                self.reopen_count = 0
                return bytes_written, fd # success

            except Exception as e:
                consecutive_errors += 1
                self.total_errors += 1

                # Check if we should abort
                if consecutive_errors >= self.max_consecutive_errors:
                    raise Exception(f"{consecutive_errors} consecutive write errors") from e

                if self.total_errors >= self.max_total_errors:
                    raise Exception(f"{self.total_errors} total write errors") from e

                # Not fatal yet - try reopening if enabled
                if self.reopen_on_error:
                    try:
                        current_pos = self.total_written
                        # Open new fd first
                        new_fd = os.open(self.device_path, os.O_WRONLY | os.O_DIRECT)
                        try:
                            # Seek to correct position on new fd
                            os.lseek(new_fd, current_pos, os.SEEK_SET)
                            # Only close old fd after new one is ready
                            old_fd = fd
                            fd = new_fd
                            try:
                                os.close(old_fd)
                            except Exception:
                                pass  # Old fd close failed, but new fd is good
                            self.reopen_count += 1
                        except Exception:
                            # New fd setup failed, close it and keep using old fd
                            os.close(new_fd)
                            raise
                    except Exception:
                        # Reopen failed - count as another error and retry with old fd
                        self.total_errors += 1

                # Retry the write (continue loop)

    def verify_partition(self, verify_pct):
        """Verify partition with section-by-section analysis"""
        # Initialize verify state
        if not self.verify_phase:
            self.verify_pct = verify_pct
            self.verify_start_mono = time.monotonic()
            self.verify_progress = 0
            self.wr_hists = []
            self.wr_hists.append(SimpleNamespace(mono=self.verify_start_mono, written=0))
            self.verify_phase = True

        if verify_pct == 0:
            self.verify_result = "skipped"
            return

        # Fast-fail for zeros
        fast_fail_zeros = (self.expected_pattern == "zeroed")

        # For unmarked disks: track if ALL bytes are zero
        all_zeros = (self.expected_pattern is None)

        # Track section results for debugging
        self.section_results = []  # Store (section_idx, result, stats)

        try:
            # Open with regular buffered I/O
            if not self.opts.dry_run:
                fd = os.open(self.device_path, os.O_RDONLY)
            else:
                fd = None

            read_chunk_size = 64 * 1024  # 64KB chunks
            SAMPLE_STEP = 23  # Sample every 23rd byte (~4% of data) - prime for even distribution

            # Skip marker area
            marker_skip = WipeTask.BUFFER_SIZE
            usable_size = self.total_size - marker_skip

            # Divide disk into 100 sections for sampling
            num_sections = 100
            section_size = usable_size // num_sections

            # Pre-allocated zero pattern for fast comparison
            ZERO_PATTERN_64K = b'\x00' * (64 * 1024)

            # Track if any section failed
            overall_failed = False
            failure_reason = ""

            for section_idx in range(num_sections):
                if self.do_abort or overall_failed:
                    break

                # Reset analysis for THIS SECTION
                section_byte_counts = [0] * 256
                section_samples = 0
                section_found_nonzero = False

                # Calculate bytes to verify in this section
                bytes_in_section = min(section_size, usable_size - section_idx * section_size)
                bytes_to_verify = int(bytes_in_section * verify_pct / 100)

                if bytes_to_verify == 0:
                    self.section_results.append((section_idx, "skipped", {}))
                    continue

                # Random offset within section
                if bytes_to_verify < bytes_in_section:
                    offset_in_section = random.randint(0, bytes_in_section - bytes_to_verify)
                else:
                    offset_in_section = 0

                read_pos = marker_skip + (section_idx * section_size) + offset_in_section
                verified_in_section = 0

                # Seek to position in this section
                if not self.opts.dry_run:
                    os.lseek(fd, read_pos, os.SEEK_SET)

                # Read and analyze THIS SECTION
                while verified_in_section < bytes_to_verify:
                    if self.do_abort:
                        break

                    chunk_size = min(read_chunk_size, bytes_to_verify - verified_in_section)

                    if self.opts.dry_run:
                        time.sleep(0.01)
                        data = b'\x00' * chunk_size
                    else:
                        data = os.read(fd, chunk_size)
                        if not data:
                            break

                    # --------------------------------------------------
                    # SECTION ANALYSIS
                    # --------------------------------------------------

                    # FAST zero check for zeroed pattern
                    if fast_fail_zeros:
                        # Ultra-fast: compare against pre-allocated zero pattern
                        if memoryview(data) != ZERO_PATTERN_64K[:len(data)]:
                            failed_offset = read_pos + verified_in_section
                            overall_failed = True
                            failure_reason = f"non-zero at {Utils.human(failed_offset)}"
                            break

                    # FAST check for unmarked disks (looking for all zeros)
                    if all_zeros and not section_found_nonzero:
                        # Fast check: use bytes.count() which is C-optimized
                        if data.count(0) != len(data):
                            section_found_nonzero = True

                    # RANDOM pattern analysis (always collect data for analysis)
                    # Use memoryview for fast slicing
                    mv = memoryview(data)
                    data_len = len(data)

                    # Sample every SAMPLE_STEP-th byte
                    for i in range(0, data_len, SAMPLE_STEP):
                        section_byte_counts[mv[i]] += 1
                        section_samples += 1

                    # --------------------------------------------------
                    # END SECTION ANALYSIS
                    # --------------------------------------------------

                    verified_in_section += len(data)
                    self.verify_progress += len(data)  # Track actual bytes read for progress

                # After reading section, analyze it
                if overall_failed:
                    break

                # Determine section result
                if fast_fail_zeros:
                    # Already passed zero check if we got here
                    section_result = "zeroed"
                    section_stats = {}

                elif all_zeros:
                    if not section_found_nonzero:
                        section_result = "zeroed"
                        section_stats = {}
                    else:
                        # Need to check if it's random
                        section_result, section_stats = self._analyze_section_randomness(
                            section_byte_counts, section_samples
                        )

                else:  # Expected random
                    section_result, section_stats = self._analyze_section_randomness(
                        section_byte_counts, section_samples
                    )

                # Store section result
                self.section_results.append((section_idx, section_result, section_stats))

                # Check if section failed
                if (self.expected_pattern == "random" and section_result != "random") or \
                   (self.expected_pattern == "zeroed" and section_result != "zeroed") or \
                   (self.expected_pattern is None and section_result == "not-wiped"):

                    overall_failed = True
                    failure_reason = f"section {section_idx}: {section_result}"
                    break

            # Close file descriptor
            if fd is not None:
                os.close(fd)

            # Determine overall result
            if overall_failed:
                if self.expected_pattern == "zeroed":
                    self.verify_result = f"not-wiped ({failure_reason})"
                elif self.expected_pattern == "random":
                    self.verify_result = f"not-wiped ({failure_reason})"
                else:  # unmarked
                    # Count section results
                    zeroed_sections = sum(1 for _, result, _ in self.section_results if result == "zeroed")
                    random_sections = sum(1 for _, result, _ in self.section_results if result == "random")
                    total_checked = len([r for _, r, _ in self.section_results if r != "skipped"])

                    if zeroed_sections == total_checked:
                        self.verify_result = "zeroed"
                        self.expected_pattern = "zeroed"
                    elif random_sections == total_checked:
                        self.verify_result = "random"
                        self.expected_pattern = "random"
                    else:
                        self.verify_result = f"mixed ({failure_reason})"
            else:
                # All sections passed
                if self.expected_pattern == "zeroed":
                    self.verify_result = "zeroed"
                elif self.expected_pattern == "random":
                    self.verify_result = "random"
                else:  # unmarked
                    # Determine from section consensus
                    zeroed_sections = sum(1 for _, result, _ in self.section_results if result == "zeroed")
                    random_sections = sum(1 for _, result, _ in self.section_results if result == "random")

                    if zeroed_sections > random_sections:
                        self.verify_result = "zeroed"
                        self.expected_pattern = "zeroed"
                    else:
                        self.verify_result = "random"
                        self.expected_pattern = "random"

        except Exception:
            self.exception = traceback.format_exc()
            self.verify_result = "error"






    def _analyze_section_randomness(self, byte_counts, total_samples):
        """Analyze if a section appears random"""
        if total_samples < 100:
            return "insufficient-data", {"samples": total_samples}

        # Calculate statistics
        max_count = max(byte_counts)
        max_freq = max_count / total_samples

        # Count unique bytes seen
        unique_bytes = sum(1 for count in byte_counts if count > 0)

        # Count completely unused bytes
        unused_bytes = sum(1 for count in byte_counts if count == 0)

        # Calculate expected frequency and variance
        expected = total_samples / 256
        if expected > 0:
            # Coefficient of variation (measure of dispersion)
            variance = sum((count - expected) ** 2 for count in byte_counts) / 256
            std_dev = variance ** 0.5
            cv = std_dev / expected
        else:
            cv = float('inf')

        # Decision logic for "random"
        # Good random data should:
        # 1. Use most byte values (>200 unique)
        # 2. No single byte dominates (<2% frequency)
        # 3. Relatively even distribution (CV < 2.0)
        # 4. Not too many zeros (if it's supposed to be random, not zeroed)

        is_random = (unique_bytes > 200 and      # >78% of bytes used
                     max_freq < 0.02 and         # No byte > 2%
                     cv < 2.0 and               # Not too lumpy
                     byte_counts[0] / total_samples < 0.5)  # Not mostly zeros

        stats = {
            "samples": total_samples,
            "max_freq": max_freq,
            "unique_bytes": unique_bytes,
            "unused_bytes": unused_bytes,
            "cv": cv,
            "zero_freq": byte_counts[0] / total_samples if total_samples > 0 else 0
        }

        if is_random:
            return "random", stats
        else:
            # Check if it's zeros
            if byte_counts[0] / total_samples > 0.95:
                return "zeroed", stats
            else:
                return "not-wiped", stats








    def _write_marker_with_verify_status(self, is_random):
        """Write marker buffer with verification status if verification was performed

        Writes marker if:
        1. Verification was performed (verify_result is set)
        2. Verification status would change from current marker, OR
        3. No marker exists but verify passed (unmarked disk that's all zeros/random)
        4. Verification was not aborted

        Args:
            is_random: bool, whether random data was written (or None to infer from verify_result)
        """
        try:
            if not self.verify_result or self.verify_result == "skipped" or self.do_abort:
                return

            # Determine verify_status based on verify_result (may include debug info)
            verify_result_base = self.verify_result.split(' ')[0] if ' ' in self.verify_result else self.verify_result

            if self.expected_pattern == "random" and verify_result_base == "random":
                new_verify_status = "pass"
            elif self.expected_pattern == "zeroed" and verify_result_base == "zeroed":
                new_verify_status = "pass"
            elif verify_result_base in ("not-wiped", "mixed", "error"):
                new_verify_status = "fail"
            else:
                # Mismatch: expected one pattern but got another
                new_verify_status = "fail"

            # Read existing marker to check if verify_status would change
            device_name = os.path.basename(self.device_path)
            existing_marker = WipeJob.read_marker_buffer(device_name)
            existing_verify_status = (getattr(existing_marker, 'verify_status', None)
                                      if existing_marker else None)

            # For unmarked disks that verified successfully, infer the mode from verify_result
            if not existing_marker and new_verify_status == "pass":
                if verify_result_base == "random":
                    is_random = True
                    self.total_written = self.total_size  # Mark as fully wiped
                elif verify_result_base == "zeroed":
                    is_random = False
                    self.total_written = self.total_size  # Mark as fully wiped
                # Write marker for this previously unmarked disk
            elif existing_marker:
                # Only write if verify status changed
                if existing_verify_status == new_verify_status:
                    return
                # Preserve original scrubbed_bytes if this is a verify-only job
                if self.total_written == 0:
                    self.total_written = existing_marker.scrubbed_bytes
            else:
                # No marker and verify failed - don't write marker
                return

            # Write marker with verification status
            if not self.opts.dry_run:
                with open(self.device_path, 'r+b') as device:
                    device.seek(0)
                    marker_buffer = self.prep_marker_buffer(is_random,
                                                            verify_status=new_verify_status)
                    device.write(marker_buffer)

        except Exception:
            # Catch ANY exception in this method to ensure self.done is always set
            self.exception = traceback.format_exc()
        finally:
            # ALWAYS set done, even if there was an exception
            self.done = True


# Initialize the class-level buffers with mmap for O_DIRECT alignment
# Now using WipeTask base class for shared buffers
if WipeTask.buffer is None:
    # Allocate random buffer with mmap (page-aligned for O_DIRECT)
    WipeTask.buffer_mem = mmap.mmap(-1, WipeTask.BUFFER_SIZE,
                                   flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    raw_buffer = os.urandom(WipeTask.BUFFER_SIZE)
    rebalanced = WipeJob._rebalance_buffer(raw_buffer)
    WipeTask.buffer_mem.write(rebalanced)
    WipeTask.buffer_mem.seek(0)
    WipeTask.buffer = memoryview(WipeTask.buffer_mem)

    # Allocate zero buffer with mmap
    WipeTask.zero_buffer_mem = mmap.mmap(-1, WipeTask.WRITE_SIZE,
                                        flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    WipeTask.zero_buffer_mem.write(b'\x00' * WipeTask.WRITE_SIZE)
    WipeTask.zero_buffer_mem.seek(0)
    WipeTask.zero_buffer = memoryview(WipeTask.zero_buffer_mem)
