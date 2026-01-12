"""
VerifyTask - Abstract base class and implementations for verification operations

Includes:
- VerifyTask: Abstract base class with verification logic and statistical analysis
- VerifyZeroTask: Concrete class for verifying zeros (fast memcmp)
- VerifyRandTask: Concrete class for verifying random data (statistical)
"""
# pylint: disable=broad-exception-raised,broad-exception-caught
import os
import time
import random
import traceback
from types import SimpleNamespace

from .WipeTask import WipeTask
from .Utils import Utils


class VerifyTask(WipeTask):
    """Abstract base class for verify operations (VerifyZeroTask, VerifyRandTask)

    Implements verification logic with:
    - Section-by-section analysis of disk content
    - Fast-fail for zero verification (memcmp)
    - Statistical analysis for random pattern verification
    - Progress tracking

    Subclasses must set:
    - expected_pattern: "zeroed" or "random"
    - fast_fail: True for zero (fast memcmp), False for random (statistical)
    """

    def __init__(self, device_path, total_size, opts=None, verify_pct=2, expected_pattern=None):
        """Initialize verify task

        Args:
            device_path: Path to device (e.g., '/dev/sda1')
            total_size: Total size in bytes
            opts: Options namespace
            verify_pct: Percentage of disk to verify (e.g., 2 for 2%)
            expected_pattern: "zeroed", "random", or None (auto-detect)
        """
        super().__init__(device_path, total_size, opts)

        # Verify-specific attributes
        self.verify_pct = verify_pct
        self.expected_pattern = expected_pattern
        self.verify_result = None  # "zeroed", "random", "not-wiped", "mixed", "error"
        self.section_results = []  # Section-by-section results
        self.verify_progress = 0  # Bytes verified (for total_written tracking)

        # Fast-fail flag (set by subclasses)
        self.fast_fail = False

    def run_task(self):
        """Execute verification operation (blocking, runs in thread)"""
        try:
            if self.verify_pct == 0:
                self.verify_result = "skipped"
                self.done = True
                return

            # Fast-fail for zeros (VerifyZeroTask)
            fast_fail_zeros = self.fast_fail and self.expected_pattern == "zeroed"

            # For unmarked disks: track if ALL bytes are zero
            all_zeros = (self.expected_pattern is None)

            # Open with regular buffered I/O
            if not self.opts.dry_run:
                fd = os.open(self.device_path, os.O_RDONLY)
            else:
                fd = None

            try:
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
                    bytes_to_verify = int(bytes_in_section * self.verify_pct / 100)

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
                        self.total_written = self.verify_progress  # Update for get_status()

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

            finally:
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

            self.done = True
        except Exception:
            self.exception = traceback.format_exc()
            self.verify_result = "error"
            self.done = True

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

    def get_status(self):
        """Get current progress status (thread-safe, called from main thread)

        Returns verification percentage with 'v' prefix (e.g., "v45%")
        """
        mono = time.monotonic()
        elapsed_time = mono - self.start_mono

        # Calculate total bytes to verify (verify_pct% of total_size)
        if self.verify_pct > 0:
            total_to_verify = self.total_size * self.verify_pct / 100
        else:
            total_to_verify = self.total_size

        # Calculate verification percentage (0-100)
        pct = int((self.verify_progress / total_to_verify) * 100) if total_to_verify > 0 else 0
        pct_str = f'v{pct}%'

        if self.do_abort:
            pct_str = 'STOP'

        # Track verification progress for rate calculation
        self.wr_hists.append(SimpleNamespace(mono=mono, written=self.verify_progress))
        floor = mono - 30
        while len(self.wr_hists) >= 3 and self.wr_hists[1].mono >= floor:
            del self.wr_hists[0]

        delta_mono = mono - self.wr_hists[0].mono
        physical_rate = (self.verify_progress - self.wr_hists[0].written) / delta_mono if delta_mono > 1.0 else 0
        # Scale rate to show "effective" verification rate (as if verifying 100% of disk)
        effective_rate = physical_rate * (100 / self.verify_pct) if self.verify_pct > 0 else physical_rate
        rate_str = f'{Utils.human(int(round(effective_rate, 0)))}/s'

        if physical_rate > 0:
            remaining = total_to_verify - self.verify_progress
            when = int(round(remaining / physical_rate))
            when_str = Utils.ago_str(when)
        else:
            when_str = '0'

        return Utils.ago_str(int(round(elapsed_time))), pct_str, rate_str, when_str

    def get_summary_dict(self):
        """Get final summary for this verify task

        Returns:
            dict: Summary with step name, elapsed, rate, bytes checked, result
        """
        mono = time.monotonic()
        elapsed = mono - self.start_mono
        rate_bps = self.verify_progress / elapsed if elapsed > 0 else 0

        # Determine mode from expected pattern
        mode = "Rand" if self.expected_pattern == "random" else "Zero"

        # Build verify label
        verify_label = f"verify {mode}"
        if self.verify_pct > 0 and self.verify_pct < 100:
            verify_label += f" ({self.verify_pct}% sample)"

        # Extract verify detail if present
        verify_detail = None
        if self.verify_result and '(' in str(self.verify_result):
            verify_detail = str(self.verify_result).split('(')[1].rstrip(')')

        result = {
            "step": verify_label,
            "elapsed": Utils.ago_str(int(elapsed)),
            "rate": f"{Utils.human(int(rate_bps))}/s",
            "bytes_checked": self.verify_progress,
            "result": self.verify_result,
        }

        if verify_detail:
            result["verify_detail"] = verify_detail

        return result


class VerifyZeroTask(VerifyTask):
    """Verify disk contains zeros"""

    def __init__(self, device_path, total_size, opts=None, verify_pct=2):
        super().__init__(device_path, total_size, opts, verify_pct, expected_pattern="zeroed")
        self.fast_fail = True  # Use fast memcmp verification

    def get_display_name(self):
        """Get display name for zero verification"""
        return "Verify"


class VerifyRandTask(VerifyTask):
    """Verify disk contains random pattern"""

    def __init__(self, device_path, total_size, opts=None, verify_pct=2):
        super().__init__(device_path, total_size, opts, verify_pct, expected_pattern="random")
        self.fast_fail = False  # Use statistical analysis

    def get_display_name(self):
        """Get display name for random verification"""
        return "Verify"
