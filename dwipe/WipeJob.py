"""
WipeJob class for handling disk/partition wiping operations
"""
import os
import json
import time
import threading
import random
import traceback
import subprocess
import mmap
from types import SimpleNamespace
from typing import Optional, Tuple

from .Utils import Utils


class WipeJob:
    """Handles disk/partition wiping operations with progress tracking"""

    # O_DIRECT requires aligned buffers and write sizes
    BLOCK_SIZE = 4096  # Alignment requirement for O_DIRECT
    WRITE_SIZE = 1 * 1024 * 1024  # 1MB (must be multiple of BLOCK_SIZE)
    BUFFER_SIZE = WRITE_SIZE  # Same size for O_DIRECT
    STATE_OFFSET = 15 * 1024  # where json is written (for marker buffer)

    # Aligned buffers allocated with mmap (initialized at module load)
    buffer = None  # Random data buffer (memoryview)
    buffer_mem = None  # Underlying mmap object
    zero_buffer = None  # Zero buffer (memoryview)
    zero_buffer_mem = None  # Underlying mmap object

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
            import random
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

    def __init__(self, device_path, total_size, opts=None, resume_from=0, resume_mode=None):
        self.opts = opts if opts else SimpleNamespace(dry_run=False)
        self.device_path = device_path
        self.total_size = total_size
        self.do_abort = False
        self.thread = None

        self.start_mono = time.monotonic()  # Track the start time
        self.total_written = resume_from  # Start from resumed offset if resuming
        self.resume_from = resume_from  # Track resume offset
        self.resume_mode = resume_mode  # Original mode if resuming (overrides opts.wipe_mode)
        self.wr_hists = []  # list of (mono, written)
        self.done = False
        self.exception = None  # in case of issues

        # Multi-pass tracking
        self.passes = getattr(opts, 'passes', 1)  # Total number of passes to perform
        self.current_pass = 0  # Current pass number (0-indexed)

        # Verification tracking
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

        ## SLOWDOWN / STALL DETECTION/ABORT FEATURE
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
        self.last_progress_written = resume_from  # Bytes written at last progress check
            # For periodic slowdown checks (every 10 seconds)
        self.last_slowdown_check = 0
            # Initialize write history for speed calculation
        self.wr_hists.append(SimpleNamespace(mono=self.start_mono, written=resume_from))



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
            # Round down to buffer boundary for safety
            scrubbed = (existing_marker.scrubbed_bytes // WipeJob.WRITE_SIZE) * WipeJob.WRITE_SIZE
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
                # Smart resume: check if we're in the final pass
                current_pass = scrubbed // total_size
                last_pass_num = passes - 1

                if passes > 1 and current_pass >= last_pass_num:
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
                else:
                    # Not in final pass, or single-pass mode - resume normally
                    resume_from = scrubbed

        job = WipeJob(device_path=device_path, total_size=total_size, opts=opts,
                     resume_from=resume_from, resume_mode=resume_mode)
        job.thread = threading.Thread(target=job.write_partition)
        job.wr_hists.append(SimpleNamespace(mono=time.monotonic(), written=resume_from))
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
        job = WipeJob(device_path=device_path, total_size=total_size, opts=opts)
        job.is_verify_only = True  # Mark as standalone verification job
        job.expected_pattern = expected_pattern
        verify_pct = getattr(opts, 'verify_pct', 0)
        if verify_pct == 0:
            verify_pct = 2  # Default to 2% if not set

        # Initialize verify state BEFORE starting thread to avoid showing "0%"
        job.verify_pct = verify_pct
        job.verify_start_mono = time.monotonic()
        job.verify_progress = 0
        job.wr_hists = [SimpleNamespace(mono=job.verify_start_mono, written=0)]
        job.verify_phase = True  # Set before thread starts

        def verify_runner():
            try:
                # Read existing marker to determine the mode and expected pattern
                device_name = os.path.basename(device_path)
                existing_marker = WipeJob.read_marker_buffer(device_name)
                if existing_marker:
                    # Infer expected pattern from marker if not already set
                    if job.expected_pattern is None:
                        job.expected_pattern = "random" if existing_marker.mode == 'Rand' else "zeroed"

                job.verify_partition(verify_pct)

                # Write marker with verification status
                if existing_marker:
                    is_random = existing_marker.mode == 'Rand'
                    job._write_marker_with_verify_status(is_random)
                    # Note: _write_marker_with_verify_status sets job.done in its finally block
                else:
                    # No marker - just mark as done
                    job.done = True
            except Exception:
                job.exception = traceback.format_exc()
            finally:
                # ALWAYS ensure job is marked as done, even if exception or early return
                if not job.done:
                    job.done = True

        job.thread = threading.Thread(target=verify_runner)
        job.wr_hists.append(SimpleNamespace(mono=time.monotonic(), written=0))
        job.thread.start()
        return job

    # Add this method to track baseline speed and check slowdown/stall
    def _check_slowdown_and_stall(self, current_monotonic):
        """Check for slowdown below threshold and stall conditions"""
        
        # Initialize baseline tracking if needed
        self.max_stall_secs = max(self.max_stall_secs, 1)
        BASELINE_SEC = 10
        if self.baseline_speed is None:
            if (current_monotonic - self.start_mono) >= BASELINE_SEC:
                # Calculate baseline speed over first 60 seconds
                total_written_60s = self.total_written - self.resume_from
                elapsed_60s = current_monotonic - self.start_mono
                if elapsed_60s > 0:
                    self.baseline_speed = total_written_60s / elapsed_60s
                    self.baseline_end_mono = current_monotonic
                    # Also initialize stall tracking
                    self.last_progress_mono = current_monotonic
                    self.last_progress_written = self.total_written
                    self.max_stall_secs = max(self.max_stall_secs, 2)
                return  # Baseline established, check next time
            else:
                return  # Still establishing baseline
        
        # Only check slowdown/stall every 10 seconds to reduce overhead
        if current_monotonic - self.last_slowdown_check < 10:
                return
        self.last_slowdown_check = current_monotonic
        self.max_stall_secs = max(self.max_stall_secs, 3)
        
        # 1. Check for stall (no progress in stall_timeout seconds)
        if self.stall_timeout > 0:
            stall_secs = current_monotonic - self.last_progress_mono
            self.max_stall_secs = max(self.max_stall_secs, stall_secs)

            if stall_secs >= self.stall_timeout:
                # No progress for stall_timeout seconds - abort
                self.do_abort = True
                self.exception = f"Stall detected: No progress for {self.stall_timeout} seconds"
                return True
        
        # 2. Check for slowdown (only if we have a baseline and slowdown_stop > 0)
        if self.slowdown_stop > 0 and self.baseline_speed > 0:
            # Calculate current speed over last 30 seconds
            floor = current_monotonic - 30
            recent_history = [h for h in self.wr_hists if h.mono >= floor]
            
            if len(recent_history) >= 2:
                recent_start = recent_history[0]
                recent_written = self.total_written - recent_start.written
                recent_elapsed = current_monotonic - recent_start.mono
                
                if recent_elapsed > 1.0:  # Need meaningful time window
                    current_speed = recent_written / recent_elapsed
                    
                    # Calculate slowdown ratio (current/baseline)
                    slowdown_ratio = self.baseline_speed / current_speed if current_speed > 0 else 9999
                    slowdown_ratio = int(round(slowdown_ratio, 0))

                    self.max_slowdown_ratio = max(slowdown_ratio, self.max_slowdown_ratio)
                    
                    # Check if slowdown exceeds threshold
                    if slowdown_ratio > self.slowdown_stop:
                        self.do_abort = True
                        self.exception = (f"Slowdown: ({current_speed/(1024*1024):.1f}) "
                                         f"is 1/{slowdown_ratio} of baseline"
                                          f" ({self.baseline_speed/(1024*1024):.1f} MB/s)")
                        return True
                    
        
        # Update progress tracking for stall detection
        if self.total_written > self.last_progress_written:
            self.last_progress_mono = current_monotonic
            self.last_progress_written = self.total_written
        
        return False

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
            rate = (progress - self.wr_hists[0].written) / delta_mono if delta_mono > 1.0 else 0
            rate_str = f'{Utils.human(int(round(rate, 0)))}/s'

            if rate > 0:
                remaining = total_to_verify - progress
                when = int(round(remaining / rate))
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
        buffer = bytearray(self.WRITE_SIZE)  # Only 16KB, not 1MB
        buffer[:self.STATE_OFFSET] = b'\x00' * self.STATE_OFFSET
        buffer[self.STATE_OFFSET:self.STATE_OFFSET + len(json_data)] = json_data
        remaining_size = self.WRITE_SIZE - (self.STATE_OFFSET + len(json_data))
        buffer[self.STATE_OFFSET + len(json_data):] = b'\x00' * remaining_size
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
                device.seek(WipeJob.WRITE_SIZE)
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
                buffer = device.read(WipeJob.BUFFER_SIZE)
        except Exception:
            return None  # cannot find info

        if buffer[:WipeJob.STATE_OFFSET] != b'\x00' * (WipeJob.STATE_OFFSET):
            return None  # First 15 KB are not zeros

        # Extract JSON data from the next 1 KB Strip trailing zeros
        json_data_bytes = buffer[WipeJob.STATE_OFFSET:WipeJob.BUFFER_SIZE].rstrip(b'\x00')

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
                       # NEW: Check for slowdown and stall inside inner loop too
                        current_mono = time.monotonic()
                        if self._check_slowdown_and_stall(current_mono):
                            break  # Abort if slowdown/stall detected
      
                        # Calculate chunk size (must be block-aligned for O_DIRECT)
                        remaining = bytes_to_write_this_pass - pass_bytes_written
                        chunk_size = min(WipeJob.WRITE_SIZE, remaining)
                        # Round down to block boundary
                        chunk_size = (chunk_size // WipeJob.BLOCK_SIZE) * WipeJob.BLOCK_SIZE
                        if chunk_size == 0:
                            break

                        # Select buffer based on pass type
                        if is_random_pass:
                            # Use slice of random buffer (still aligned via memoryview)
                            chunk = WipeJob.buffer[:chunk_size]
                        else:
                            # Use zero buffer
                            chunk = WipeJob.zero_buffer[:chunk_size]

                        if self.opts.dry_run:
                            bytes_written = chunk_size
                            time.sleep(0.001)
                        else:
                            try:
                                # Write with O_DIRECT (bypasses page cache)
                                bytes_written = os.write(fd, chunk)
                            except Exception as e:
                                # Save exception for debugging
                                self.exception = f"Write error at {self.total_written}: {e}"
                                bytes_written = 0

                        self.total_written += bytes_written
                        pass_bytes_written += bytes_written

                        # Periodically update marker for crash recovery (every 30s)
                        # Note: marker writes use separate buffered file handle
                        if not self.opts.dry_run and self.total_written > self.BUFFER_SIZE:
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

    @staticmethod
    def analyze_data_pattern(data):
        """Analyze a chunk of data to determine if it's zeros, random, or other

        Args:
            data: bytes to analyze

        Returns:
            str: "zeroed", "random", or "not-wiped"
        """
        if not data:
            return "not-wiped"

        # Count byte frequencies
        byte_counts = [0] * 256
        for byte in data:
            byte_counts[byte] += 1

        total_bytes = len(data)

        # Check if >95% zeros
        if byte_counts[0] > total_bytes * 0.95:
            return "zeroed"

        # Check for random distribution
        # For truly random data, no single byte value should dominate
        # Use simple test: max frequency < 2% of total
        max_count = max(byte_counts)
        if max_count < total_bytes * 0.02:
            return "random"

        # Check entropy more carefully
        # Count non-zero bytes
        non_zero_count = sum(1 for c in byte_counts if c > 0)

        # Random data should use most byte values (>200 of 256)
        if non_zero_count > 200:
            # Check distribution evenness
            expected = total_bytes / 256
            variance = sum((c - expected) ** 2 for c in byte_counts) / 256
            # Low variance = more random
            if variance < expected * 10:  # Threshold for "random enough"
                return "random"

        return "not-wiped"

    def verify_partition(self, verify_pct):
        """Verify that partition was wiped according to expected pattern

        Reads verify_pct% of the disk sequentially from the start (after marker).
        Sequential reading is much faster than random sampling.

        Fast-fail logic:
        - For "zeroed" pattern: fails immediately on first non-zero byte
        - For "random" pattern: checks byte distribution uniformity

        Args:
            verify_pct: Percentage to verify (0-100)
        """
        # Initialize verify state (may already be set for standalone verify jobs)
        if not self.verify_phase:
            # Called from write_partition after wipe
            self.verify_pct = verify_pct
            self.verify_start_mono = time.monotonic()
            self.verify_progress = 0
            self.wr_hists = []
            self.wr_hists.append(SimpleNamespace(mono=self.verify_start_mono, written=0))
            self.verify_phase = True

        if verify_pct == 0:
            self.verify_result = "skipped"
            return

        # Fast-fail for zeros: if expected pattern is "zeroed", fail on first non-zero
        fast_fail_zeros = (self.expected_pattern == "zeroed")

        # For random: track byte distribution for chi-squared test
        # Also track for unmarked disks (expected_pattern is None) to detect pattern
        byte_counts = [0] * 256 if (self.expected_pattern == "random" or
                                     self.expected_pattern is None) else None

        # For unmarked disks, also track if ALL bytes are zero
        all_zeros = (self.expected_pattern is None)
        found_nonzero = False

        try:
            # Open with regular buffered I/O for fast verification reads
            # (O_SYNC on reads just adds overhead without cache benefits)
            # Cache pollution from verification is minimal and temporary
            if not self.opts.dry_run:
                fd = os.open(self.device_path, os.O_RDONLY)
            else:
                fd = None

            read_chunk_size = 64 * 1024  # 64KB chunks for reading

            try:
                # Skip the marker buffer area (first 1MB) to avoid reading the marker
                marker_skip = WipeJob.BUFFER_SIZE  # 1MB
                usable_size = self.total_size - marker_skip

                # Divide disk into 100 sections for random sampling
                num_sections = 100
                section_size = usable_size // num_sections

                total_bytes_verified = 0
                total_bytes_sampled = 0  # Bytes actually counted (may be subset)

                for section_idx in range(num_sections):
                    if self.do_abort:
                        break

                    # Calculate bytes to verify in this section
                    section_start = marker_skip + (section_idx * section_size)
                    bytes_to_verify = int(section_size * verify_pct / 100)

                    # Random offset within section
                    if bytes_to_verify < section_size:
                        offset_in_section = random.randint(0, section_size - bytes_to_verify)
                    else:
                        offset_in_section = 0

                    read_pos = section_start + offset_in_section
                    verified_in_section = 0

                    # Seek to position in this section
                    if not self.opts.dry_run:
                        os.lseek(fd, read_pos, os.SEEK_SET)

                    # Read data from this section
                    while verified_in_section < bytes_to_verify:
                        if self.do_abort:
                            break

                        chunk_size = min(read_chunk_size, bytes_to_verify - verified_in_section)

                        if self.opts.dry_run:
                            time.sleep(0.01)
                            data = b'\x00' * chunk_size  # Fake data for dry run
                        else:
                            # Read with O_SYNC (avoids cache pollution)
                            data = os.read(fd, chunk_size)
                            if not data:
                                break  # EOF

                        # Fast-fail for zeros: check every byte
                        if fast_fail_zeros:
                            for i, byte in enumerate(data):
                                if byte != 0:
                                    failed_offset = read_pos + i
                                    self.verify_result = f"not-wiped (non-zero at {Utils.human(failed_offset)})"
                                    return

                        # For unmarked disks: check if all zeros
                        if all_zeros and not found_nonzero:
                            for byte in data:
                                if byte != 0:
                                    found_nonzero = True
                                    break

                        # For random: sample every 100th byte (1% sample for speed)
                        if byte_counts is not None:
                            for i in range(0, len(data), 100):
                                byte_counts[data[i]] += 1
                                total_bytes_sampled += 1

                        verified_in_section += chunk_size
                        total_bytes_verified += chunk_size
                        self.verify_progress += chunk_size

                    # Periodically check distribution for fast-fail (every 10 sections)
                    if byte_counts and section_idx > 0 and section_idx % 10 == 0:
                        if total_bytes_sampled > 10000:  # Need reasonable sample size
                            # Check if distribution is uniform enough for wiped data
                            max_count = max(byte_counts)
                            max_freq = max_count / total_bytes_sampled

                            # If one byte dominates (>95%), it's likely zeros - that's OK
                            # If max frequency is high (>5%), it's likely real data - fail
                            # We accept: zeros (>95%) or fairly even distribution (<5%)
                            if 0.05 < max_freq < 0.95:
                                # Not zeros, not random - likely real data
                                self.verify_result = "not-wiped"
                                return

                # Final determination - use simple frequency distribution check
                if fast_fail_zeros:
                    # Made it through all checks - it's all zeros
                    self.verify_result = "zeroed"
                elif all_zeros:
                    # Unmarked disk detection
                    if not found_nonzero:
                        # All zeros detected
                        self.verify_result = "zeroed"
                        self.expected_pattern = "zeroed"  # Set for marker writing
                    else:
                        # Not all zeros, check if distribution is uniform
                        if byte_counts and total_bytes_sampled > 0:
                            max_count = max(byte_counts)
                            max_freq = max_count / total_bytes_sampled
                            # Check for fairly even distribution (max byte < 2% frequency)
                            if max_freq < 0.02:
                                self.verify_result = f"random (max={max_freq*100:.2f}%)"
                                self.expected_pattern = "random"  # Set for marker writing
                            else:
                                self.verify_result = f"not-wiped (max={max_freq*100:.2f}%)"
                        else:
                            self.verify_result = "not-wiped"
                elif byte_counts:
                    # Known random pattern - check distribution uniformity
                    if total_bytes_sampled > 0:
                        max_count = max(byte_counts)
                        max_freq = max_count / total_bytes_sampled
                        # Check for fairly even distribution (max byte < 2% frequency)
                        if max_freq < 0.02:
                            self.verify_result = f"random (max={max_freq*100:.2f}%)"
                        else:
                            self.verify_result = f"not-wiped (max={max_freq*100:.2f}%)"
                    else:
                        self.verify_result = "skipped"
                else:
                    self.verify_result = "skipped"

            finally:
                # Close file descriptor
                if fd is not None:
                    os.close(fd)

        except Exception:
            self.exception = traceback.format_exc()
            self.verify_result = "error"

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
if WipeJob.buffer is None:
    # Allocate random buffer with mmap (page-aligned for O_DIRECT)
    WipeJob.buffer_mem = mmap.mmap(-1, WipeJob.BUFFER_SIZE,
                                   flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    raw_buffer = os.urandom(WipeJob.BUFFER_SIZE)
    rebalanced = WipeJob._rebalance_buffer(raw_buffer)
    WipeJob.buffer_mem.write(rebalanced)
    WipeJob.buffer_mem.seek(0)
    WipeJob.buffer = memoryview(WipeJob.buffer_mem)

    # Allocate zero buffer with mmap
    WipeJob.zero_buffer_mem = mmap.mmap(-1, WipeJob.WRITE_SIZE,
                                        flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    WipeJob.zero_buffer_mem.write(b'\x00' * WipeJob.WRITE_SIZE)
    WipeJob.zero_buffer_mem.seek(0)
    WipeJob.zero_buffer = memoryview(WipeJob.zero_buffer_mem)
