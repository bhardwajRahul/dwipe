"""
FirmwareWipeTask - Firmware-based secure erase operations

Includes:
- FirmwareWipeTask: Abstract base class for firmware wipes
- NvmeWipeTask: NVMe secure erase using nvme-cli
- SataWipeTask: SATA/ATA secure erase using hdparm
"""
# pylint: disable=broad-exception-raised,broad-exception-caught
# pylint: disable=invalid-name,too-many-instance-attributes,too-many-arguments
# pylint: disable=too-many-positional-arguments,consider-using-with
# pylint: disable=too-many-return-statements
import os
import json
import time
import random
import subprocess
import traceback

from .WipeTask import WipeTask
from .Utils import Utils
from .SataTool import SataTool
from .NvmeTool import NvmeTool


class FirmwareWipeTask(WipeTask):
    """Abstract base class for firmware-based wipe operations

    Firmware wipes execute in the drive's controller, not via CPU writes.
    The host just sends a command and monitors for completion.

    Progress reporting is estimated since most firmware doesn't report
    real-time progress. NVMe sanitize can optionally poll for actual progress.

    Subclasses must implement:
    - _build_command(): Return list of command args
    - _check_completion(): Check if wipe is complete
    - _estimate_duration(): Estimate total time in seconds
    """

    def __init__(self, device_path, total_size, opts, command_args, wipe_name):
        """Initialize firmware wipe task

        Args:
            device_path: Path to device (e.g., '/dev/sda', '/dev/nvme0n1')
            total_size: Total size in bytes
            opts: Options namespace
            command_args: Command args from hw_caps (e.g., 'sanitize --action=0x04')
            wipe_name: Human-readable name (e.g., 'Sanitize-Crypto')
        """
        super().__init__(device_path, total_size, opts)

        self.command_args = command_args
        self.wipe_name = wipe_name
        self.process = None
        self.finish_mono = None
        self.actual_command = None  # Store actual command that was executed
        self.return_code = None     # Store process return code
        self.elapsed_secs = 0       # Capture elapsed time when task completes

        # Estimated duration for progress reporting
        self.estimated_duration = self._estimate_duration()

    def _estimate_duration(self):
        """Estimate total duration in seconds (override in subclasses)

        Returns:
            int: Estimated seconds for completion
        """
        return 60  # Default 1 minute

    def get_display_name(self):
        """Get display name for firmware wipe"""
        return self.wipe_name

    def _build_command(self):
        """Build command list for subprocess (must be implemented by subclasses)

        Returns:
            list: Command args like ['nvme', 'sanitize', ...]
        """
        raise NotImplementedError("Subclasses must implement _build_command()")

    def _check_completion(self):
        """Check if wipe completed successfully (can be overridden)

        Returns:
            bool or None: True if done, False if failed, None if still running
        """
        if self.process and self.process.poll() is not None:
            return self.process.returncode == 0
        return None

    def _write_marker(self):
        """Write completion marker after firmware wipe

        Firmware wipes erase the entire disk including any existing markers.
        We need to write a new marker indicating the wipe is complete.
        """
        try:
            # Force OS to re-read partition table (now empty)
            subprocess.run(['blockdev', '--rereadpt', self.device_path],
                          capture_output=True, timeout=5, check=False)
            time.sleep(1)  # Let kernel settle

            # Prepare marker data
            data = {
                "unixtime": int(time.time()),
                "scrubbed_bytes": self.total_size,
                "size_bytes": self.total_size,
                "passes": 1,
                "mode": self.wipe_name,  # e.g., 'Crypto', 'Enhanced'
            }
            json_data = json.dumps(data).encode('utf-8')

            # Build marker buffer (16KB)
            buffer = bytearray(WipeTask.MARKER_SIZE)
            buffer[:WipeTask.STATE_OFFSET] = b'\x00' * WipeTask.STATE_OFFSET
            buffer[WipeTask.STATE_OFFSET:WipeTask.STATE_OFFSET + len(json_data)] = json_data
            remaining = WipeTask.MARKER_SIZE - (WipeTask.STATE_OFFSET + len(json_data))
            buffer[WipeTask.STATE_OFFSET + len(json_data):] = b'\x00' * remaining

            # Write marker to beginning of device
            with open(self.device_path, 'wb') as f:
                f.write(buffer)
                f.flush()
                os.fsync(f.fileno())

        except Exception as e:
            # Don't fail the whole job if marker write fails
            self.exception = f"Marker write warning: {e}"

    def run_task(self):
        """Execute firmware wipe operation (blocking, runs in thread)"""
        try:
            # Build command
            cmd = self._build_command()
            self.actual_command = cmd  # Store command for logging

            # Start subprocess (non-blocking) - skip if process already started (e.g., NVMe)
            if cmd:
                self.process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

            # Monitor progress with polling loop
            check_interval = 2  # Check every 2 seconds

            while not self.do_abort:
                # Check if process completed
                completion_status = self._check_completion()

                if completion_status is True:
                    # Success!
                    self.total_written = self.total_size
                    self.finish_mono = time.monotonic()

                    # Capture return code
                    if self.process:
                        self.return_code = self.process.returncode

                    # Write marker after successful firmware wipe
                    self._write_marker()
                    break

                if completion_status is False:
                    # Failed
                    stderr = self.process.stderr.read() if self.process.stderr else ""
                    self.exception = f"Firmware wipe failed: {stderr}"

                    # Capture return code on failure too
                    if self.process:
                        self.return_code = self.process.returncode
                    break

                # Still running - update estimated progress
                elapsed = time.monotonic() - self.start_mono
                progress_pct = min(1.0, elapsed / self.estimated_duration)
                self.total_written = int(self.total_size * progress_pct)

                time.sleep(check_interval)

            # Handle abort
            if self.do_abort and self.process:
                self.process.terminate()
                time.sleep(0.5)
                if self.process.poll() is None:
                    self.process.kill()

        except Exception:
            self.exception = traceback.format_exc()
        finally:
            # Capture elapsed time when task completes
            self.elapsed_secs = int(time.monotonic() - self.start_mono)
            self.done = True

    def get_status(self):
        """Get current progress status (thread-safe)

        Returns:
            tuple: (elapsed_str, pct_str, rate_str, eta_str, more_state)
        """
        mono = time.monotonic()
        elapsed_time = mono - self.start_mono

        # Calculate percentage based on estimated progress
        pct = (self.total_written / self.total_size) * 100 if self.total_size > 0 else 0
        pct = min(pct, 100)
        pct_str = f'{int(round(pct))}%'

        if self.do_abort:
            pct_str = 'STOP'

        # Show "FW" to indicate firmware operation
        rate_str = 'FW'

        # Calculate ETA based on estimated duration
        if pct < 100:
            remaining = self.estimated_duration - elapsed_time
            eta_str = Utils.ago_str(max(0, int(remaining)))
        else:
            eta_str = '0'

        elapsed_str = Utils.ago_str(int(round(elapsed_time)))

        return elapsed_str, pct_str, rate_str, eta_str, self.more_state

    def get_summary_dict(self):
        """Generate summary dictionary for structured logging

        Returns:
            dict: Summary with step details including command sequence and return codes
        """
        summary = {
            "step": f"firmware {self.wipe_name} {self.device_path}",
            "elapsed": Utils.ago_str(self.elapsed_secs),
            "rate": "Firmware",
            "bytes_written": self.total_written,
            "bytes_total": self.total_size,
            "result": "completed" if self.total_written == self.total_size else "partial"
        }

        # Include actual command if available
        if self.actual_command:
            summary["command"] = ' '.join(self.actual_command) if isinstance(self.actual_command, list) else str(self.actual_command)

        # Include return code if process completed
        if self.return_code is not None:
            summary["return_code"] = self.return_code

        return summary


class NvmeWipeTask(FirmwareWipeTask):
    """NVMe firmware wipe using NvmeTool

    Supports various sanitize and format operations:
    - Sanitize: Crypto Erase, Block Erase, Overwrite
    - Format: Crypto Erase, User Data Erase

    Example command_args:
    - 'sanitize_crypto' (Sanitize with Crypto Erase)
    - 'sanitize_block' (Sanitize with Block Erase)
    - 'format_erase' (Format with Crypto Erase)
    """
    def __init__(self, device_path, total_size, opts, command_args, wipe_name):
        self.tool = NvmeTool(device_path)
        self.wipe_method = command_args  # e.g., 'sanitize_crypto', 'sanitize_block', 'format_erase'
        self.state_mono = 0
        super().__init__(device_path, total_size, opts, command_args, wipe_name)

    def _estimate_duration(self):
        """Estimate NVMe wipe duration from tool capabilities

        Most NVMe sanitize/format operations complete in seconds.
        Crypto erase: 2-10 seconds
        Block erase: 10-30 seconds
        Overwrite: 30-120 seconds
        """
        if self.tool.job.est_secs:
            return self.tool.job.est_secs

        # Fallback estimates based on method
        if 'crypto' in self.wipe_method:
            return 10
        if 'block' in self.wipe_method:
            return 30
        if 'overwrite' in self.wipe_method:
            return 120
        return 30

    def _build_command(self):
        """Build NVMe wipe command using NvmeTool

        Returns:
            None (NvmeTool handles command internally)
        """
        # Verify capabilities for the specific wipe method
        verdict = self.tool.get_wipe_verdict(method=self.wipe_method)
        if verdict != "OK":
            raise Exception(f"NVMe pre-flight failed: {verdict}")

        # Start wipe using NvmeTool
        self.tool.start_wipe(method=self.wipe_method)
        self.process = self.tool.job.process
        self.more_state = 'nvmeIP'

        # Return empty list since process is already started
        return []

    def _check_completion(self):
        """Check NVMe wipe completion with real-time progress polling"""
        if self.more_state == 'nvmeIP':
            if not self.process:
                self.more_state = 'no-process'
                return False

            # For sanitize operations, poll sanitize-log for actual progress
            if 'sanitize' in self.wipe_method:
                status, percent = self.tool.get_sanitize_status()
                if status is not None:
                    # Update progress based on actual sanitize status
                    self.total_written = int(self.total_size * (percent / 100.0))

                    # sstat values: 0=Idle (done or not started), 1=In Progress, 2=Success, 3=Failed
                    if status == 0 or status == 2:
                        # Check if process exited successfully
                        if self.process.poll() is not None:
                            if self.process.returncode == 0:
                                self.more_state = 'Complete'
                                return True
                            else:
                                self.more_state = f'rv={self.process.returncode}'
                                return False
                    elif status == 3:
                        self.more_state = 'sanitize-failed'
                        return False
                    # status == 1: still in progress
                    return None

            # For format operations, just check if process completed
            if self.process.poll() is not None:
                if self.process.returncode == 0:
                    self.more_state = 'Complete'
                    return True
                else:
                    self.more_state = f'rv={self.process.returncode}'
                    return False
            return None

        if self.more_state == 'Complete':
            return True

        return False


class SataWipeTask(FirmwareWipeTask):
    """SATA/ATA firmware wipe using hdparm

    Uses ATA Security Erase command:
    - Normal Erase: Writes zeros to all sectors (slow)
    - Enhanced Erase: Cryptographic erase or vendor-specific (fast)

    Example command_args:
    - '--user-master u --security-erase NULL'
    - '--user-master u --security-erase-enhanced NULL'

    Note: Requires setting a temporary password before erase.
    """
    def __init__(self, device_path, total_size, opts, command_args, wipe_name):
        self.tool = SataTool(device_path)
        self.use_enhanced = 'enhanced' in command_args
        self.state_mono = 0
        self.commands_executed = []  # Store pre-erase commands for logging
        super().__init__(device_path, total_size, opts, command_args, wipe_name)

    def _estimate_duration(self):
        """Estimate SATA erase duration from drive's reported time"""
        self.tool.refresh_secures()
        if self.tool.secures and self.tool.secures.erase_est_secs:
            idx = -1 if self.use_enhanced else 0
            return self.tool.secures.erase_est_secs[idx]
        return 4 * 60 * 60  # Default 4 hours for SATA

    def _build_command(self):
        """Build hdparm erase command (sets password first as required for SATA)

        Uses SataTool.start_wipe() to execute pre-erase commands and get the final erase command.

        Returns:
            list: ['hdparm', '--user-master', 'u', '--security-erase', 'NULL', '/dev/sda']
        """
        # Call start_wipe which handles password setting and pre-erase setup
        result = self.tool.start_wipe(use_enhanced=self.use_enhanced, password='NULL', db=False)

        # Handle error case (returns tuple)
        if isinstance(result, tuple):
            _, message = result
            raise Exception(f"SATA wipe setup failed: {message}")

        # Handle success case (returns dict with command sequence)
        if isinstance(result, dict):
            # Capture pre-erase commands for logging
            self.commands_executed = result.get('commands_executed', [])
            erase_cmd = result.get('erase_command', [])

            # Check if we got valid erase command
            if not erase_cmd:
                raise Exception("start_wipe() did not return erase command")

            self.more_state = 'hdparmIP'
            return erase_cmd

        # Fallback for unexpected return type
        raise Exception(f"Unexpected return from start_wipe(): {type(result)}")

    def get_summary_dict(self):
        """Generate summary dict with SATA pre-erase command sequence included"""
        summary = super().get_summary_dict()

        # Add pre-erase commands executed (for convincing evidence)
        if self.commands_executed:
            summary["commands_executed"] = self.commands_executed

        return summary

    def _check_completion(self):
        """Check SATA erase completion and verify result"""
        if self.more_state == 'hdparmIP':
            if not self.process:
                self.more_state = 'no-self.process'
                return False
            if self.process.poll() is None:
                return None # still running, same state
            if self.process.returncode != 0:
                self.more_state = f'rv={self.process.returncode}'
                return False
            self.more_state = 'NotReady' # move on
        if self.more_state == 'NotReady':
            rv, why = self.tool.verify_wipe_result()
            if not rv:
                self.more_state = why
                return None
            self.more_state = 'Pause'
            self.state_mono = time.monotonic()
        if self.more_state == 'Pause':
            if time.monotonic() - self.state_mono < 5.0:
                return None # keep waiting
            self.more_state = 'Complete'
            return True

        return False # unexpected state


class StandardPrecheckTask(WipeTask):
    """Precheck firmware wipe capabilities before proceeding

    Validates that the selected wipe method is available on the device.
    Applies to both NVMe and SATA firmware wipes.
    """

    def __init__(self, device_path, total_size, opts, selected_wipe_type):
        super().__init__(device_path, total_size, opts)
        self.selected_wipe_type = selected_wipe_type
        self.capabilities_found = {}
        self.method_available = False
        self.elapsed_secs = 0

    def get_display_name(self):
        """Get display name for precheck task"""
        return "Precheck"

    def run_task(self):
        """Validate that selected wipe method is available on device"""
        try:
            # Determine device type and check capabilities
            if self.device_path.startswith('/dev/nvme'):
                # NVMe device
                tool = NvmeTool(self.device_path)
                # Check if selected method is available
                verdict = tool.get_wipe_verdict(method=self.selected_wipe_type)
                if verdict == "OK":
                    self.capabilities_found[self.selected_wipe_type] = "available"
                    self.method_available = True
                else:
                    self.capabilities_found[self.selected_wipe_type] = verdict
                    self.method_available = False
            else:
                # SATA device
                tool = SataTool(self.device_path)
                tool.refresh_secures()
                if tool.secures and tool.secures.supported:
                    self.capabilities_found[self.selected_wipe_type] = "available"
                    self.method_available = True
                else:
                    self.capabilities_found[self.selected_wipe_type] = "not_available"
                    self.method_available = False

            if not self.method_available:
                self.exception = f"Selected wipe method '{self.selected_wipe_type}' not available"

        except Exception:
            if not self.exception:
                self.exception = traceback.format_exc()
        finally:
            # Capture elapsed time when task completes
            self.elapsed_secs = int(time.monotonic() - self.start_mono)
            self.done = True

    def get_summary_dict(self):
        """Generate summary dictionary for structured logging"""
        return {
            "step": f"precheck firmware capabilities {self.device_path}",
            "elapsed": Utils.ago_str(self.elapsed_secs),
            "rate": "Precheck",
            "capabilities_found": self.capabilities_found,
            "selected_method": self.selected_wipe_type,
            "result": "passed" if self.method_available else "failed"
        }


class FirmwarePreVerifyTask(WipeTask):
    """Write test blocks before firmware wipe for later verification

    Writes 3 x 4KB blocks of pseudo-random data at:
    - Front: 16KB after marker area
    - Middle: total_size // 2
    - End: total_size - 4096

    Stores blocks in self.original_blocks for post-verify to compare against.
    """

    BLOCK_SIZE = 4096
    NUM_BLOCKS = 3

    def __init__(self, device_path, total_size, opts):
        super().__init__(device_path, total_size, opts)
        self.blocks_written = 0
        self.original_blocks = []  # Stores 3 x 4KB test blocks
        self.test_locations = []   # Stores 3 offsets
        self.elapsed_secs = 0      # Capture elapsed time when task completes

    def get_display_name(self):
        """Get display name for pre-verify task"""
        return "Pre-verify"

    def run_task(self):
        """Write 3 test blocks at front/middle/end and save for later verification"""
        try:
            # Calculate locations (skip marker area at front)
            self.test_locations = [
                WipeTask.MARKER_SIZE + 16384,    # Front: 16KB after marker
                self.total_size // 2,             # Middle
                self.total_size - self.BLOCK_SIZE # End
            ]

            # Generate pseudo-random test data
            random.seed(int(time.time()))
            for i in range(self.NUM_BLOCKS):
                block = bytes([random.randint(0, 255) for _ in range(self.BLOCK_SIZE)])
                self.original_blocks.append(block)

            # Write blocks to device
            with open(self.device_path, 'r+b') as device:
                for i, offset in enumerate(self.test_locations):
                    if self.do_abort:
                        break

                    device.seek(offset)
                    device.write(self.original_blocks[i])
                    device.flush()
                    self.blocks_written += 1
                    self.total_written = self.blocks_written * self.BLOCK_SIZE

                os.fsync(device.fileno())

        except Exception:
            self.exception = traceback.format_exc()
        finally:
            # Capture elapsed time when task completes (not when logged later)
            self.elapsed_secs = int(time.monotonic() - self.start_mono)
            self.done = True

    def get_summary_dict(self):
        """Generate summary dictionary for structured logging"""
        return {
            "step": "pre-verify test blocks",
            "elapsed": Utils.ago_str(self.elapsed_secs),
            "rate": "Test",
            "blocks_written": self.blocks_written,
            "result": "completed" if self.blocks_written == self.NUM_BLOCKS else "partial"
        }


class FirmwarePostVerifyTask(WipeTask):
    """Verify firmware wipe effectiveness by checking if test blocks changed

    Reads the 3 test blocks written by FirmwarePreVerifyTask and verifies
    that at least 95% of bytes have changed (indicating effective wipe).

    Reads block data from the pre-verify task via self.job.tasks.
    """

    THRESHOLD_PCT = 95.0  # Require 95%+ bytes different

    def __init__(self, device_path, total_size, opts):
        super().__init__(device_path, total_size, opts)
        self.diff_percentages = []
        self.verification_passed = False
        self.elapsed_secs = 0      # Capture elapsed time when task completes

    def get_display_name(self):
        """Get display name for post-verify task"""
        return "Post-verify"

    def _update_marker_verify_status(self, verify_status):
        """Update the wipe marker with verification status (pass/fail)

        Reads the existing marker written by FirmwareWipeTask, updates it with
        the verification result, and writes it back. This provides the checkmark
        display in the UI when verification passes.
        """
        try:
            device_name = self.device_path.replace('/dev/', '')

            # Read existing marker
            from .WipeJob import WipeJob
            marker = WipeJob.read_marker_buffer(device_name)
            if not marker:
                return  # No marker to update

            # Prepare updated marker data
            data = {
                "unixtime": marker.unixtime,
                "scrubbed_bytes": marker.scrubbed_bytes,
                "size_bytes": marker.size_bytes,
                "passes": marker.passes,
                "mode": marker.mode,
                "verify_status": verify_status,  # "pass" or "fail"
            }

            json_data = json.dumps(data).encode('utf-8')

            # Build marker buffer (16KB)
            buffer = bytearray(WipeTask.MARKER_SIZE)
            buffer[:WipeTask.STATE_OFFSET] = b'\x00' * WipeTask.STATE_OFFSET
            buffer[WipeTask.STATE_OFFSET:WipeTask.STATE_OFFSET + len(json_data)] = json_data
            remaining = WipeTask.MARKER_SIZE - (WipeTask.STATE_OFFSET + len(json_data))
            buffer[WipeTask.STATE_OFFSET + len(json_data):] = b'\x00' * remaining

            # Write updated marker to beginning of device
            with open(self.device_path, 'wb') as f:
                f.write(buffer)
                f.flush()
                os.fsync(f.fileno())

        except Exception:
            # Don't fail the job if marker update fails
            pass

    def run_task(self):
        """Read test blocks and verify they differ from originals by 95%+"""
        try:
            # Find pre-verify task in job's task list
            pre_verify_task = None
            for task in self.job.tasks:
                if isinstance(task, FirmwarePreVerifyTask):
                    pre_verify_task = task
                    break

            if not pre_verify_task or not pre_verify_task.original_blocks:
                raise Exception("No pre-verify test blocks found")

            original_blocks = pre_verify_task.original_blocks
            test_locations = pre_verify_task.test_locations
            block_size = FirmwarePreVerifyTask.BLOCK_SIZE

            # Read current blocks from device
            with open(self.device_path, 'rb') as device:
                for i, offset in enumerate(test_locations):
                    if self.do_abort:
                        break

                    device.seek(offset)
                    current_block = device.read(block_size)

                    if len(current_block) != block_size:
                        raise Exception(f"Failed to read block {i} at offset {offset}")

                    # Calculate bytes that differ
                    diff_bytes = sum(1 for j in range(block_size)
                                    if current_block[j] != original_blocks[i][j])

                    diff_pct = (diff_bytes / block_size) * 100.0
                    self.diff_percentages.append(round(diff_pct, 1))
                    self.total_written = (i + 1) * block_size

            # Check if all blocks meet threshold
            self.verification_passed = all(pct >= self.THRESHOLD_PCT
                                          for pct in self.diff_percentages)

            if not self.verification_passed:
                min_pct = min(self.diff_percentages)
                self.exception = (f"Firmware wipe verification FAILED: "
                                f"Test block changed only {min_pct}% "
                                f"(threshold: {self.THRESHOLD_PCT}%)")

            # Update marker with verification status
            verify_status = "pass" if self.verification_passed else "fail"
            self._update_marker_verify_status(verify_status)

        except Exception:
            if not self.exception:
                self.exception = traceback.format_exc()
        finally:
            # Capture elapsed time when task completes (not when logged later)
            self.elapsed_secs = int(time.monotonic() - self.start_mono)
            self.done = True

    def get_summary_dict(self):
        """Generate summary dictionary for structured logging"""
        return {
            "step": "post-verify test blocks",
            "elapsed": Utils.ago_str(self.elapsed_secs),
            "rate": "Test",
            "diff_pct": self.diff_percentages,
            "result": "pass" if self.verification_passed else "fail"
        }
