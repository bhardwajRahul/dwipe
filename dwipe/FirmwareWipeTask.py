"""
FirmwareWipeTask - Firmware-based secure erase operations

Includes:
- FirmwareWipeTask: Abstract base class for firmware wipes
- NvmeWipeTask: NVMe secure erase using nvme-cli
- SataWipeTask: SATA/ATA secure erase using hdparm
"""
# pylint: disable=broad-exception-raised,broad-exception-caught
import os
import json
import time
import subprocess
import traceback
# from types import SimpleNamespace

from .WipeTask import WipeTask
from .Utils import Utils


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

        # Estimated duration for progress reporting
        self.estimated_duration = self._estimate_duration()

    def _estimate_duration(self):
        """Estimate total duration in seconds (override in subclasses)

        Returns:
            int: Estimated seconds for completion
        """
        return 60  # Default 1 minute

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
        if self.opts.dry_run:
            return

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
                "mode": self.wipe_name,  # e.g., 'Sanitize-Crypto'
                "firmware_wipe": True
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

            # Start subprocess (non-blocking)
            if not self.opts.dry_run:
                self.process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
            else:
                # Dry run - simulate quick completion
                time.sleep(2)
                self.total_written = self.total_size
                self.done = True
                return

            # Monitor progress with polling loop
            check_interval = 2  # Check every 2 seconds

            while not self.do_abort:
                # Check if process completed
                completion_status = self._check_completion()

                if completion_status is True:
                    # Success!
                    self.total_written = self.total_size
                    self.finish_mono = time.monotonic()

                    # Write marker after successful firmware wipe
                    self._write_marker()
                    break

                elif completion_status is False:
                    # Failed
                    stderr = self.process.stderr.read() if self.process.stderr else ""
                    self.exception = f"Firmware wipe failed: {stderr}"
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
            self.done = True

    def get_status(self):
        """Get current progress status (thread-safe)

        Returns:
            tuple: (elapsed_str, pct_str, rate_str, eta_str)
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

        return elapsed_str, pct_str, rate_str, eta_str

    def get_summary_dict(self):
        """Generate summary dictionary for structured logging

        Returns:
            dict: Summary with step details
        """
        mono = time.monotonic()
        elapsed = mono - self.start_mono

        return {
            "step": f"firmware {self.wipe_name} {self.device_path}",
            "elapsed": Utils.ago_str(int(elapsed)),
            "rate": "Firmware",
            "command": ' '.join(self._build_command()) if not self.opts.dry_run else "dry-run",
            "bytes_written": self.total_written,
            "bytes_total": self.total_size,
            "result": "completed" if self.total_written == self.total_size else "partial"
        }


class NvmeWipeTask(FirmwareWipeTask):
    """NVMe firmware wipe using nvme-cli

    Supports various sanitize and format operations:
    - Sanitize: Crypto Erase, Block Erase, Overwrite
    - Format: Crypto Erase, User Data Erase

    Example command_args:
    - 'sanitize --action=0x04' (Crypto Erase)
    - 'format --ses=2' (Format with Crypto Erase)
    """

    def _estimate_duration(self):
        """Estimate NVMe wipe duration

        Most NVMe sanitize/format operations complete in seconds.
        Crypto erase: 2-10 seconds
        Block erase: 10-30 seconds
        Overwrite: 30-120 seconds
        """
        if 'sanitize' in self.command_args:
            if 'crypto' in self.command_args or '0x04' in self.command_args:
                return 10  # Crypto erase is very fast
            elif 'block' in self.command_args or '0x02' in self.command_args:
                return 30
            else:  # Overwrite
                return 120
        else:  # Format
            return 30

    def _build_command(self):
        """Build nvme command

        Returns:
            list: ['nvme', 'sanitize', '--action=0x04', '/dev/nvme0n1']
        """
        # Parse command_args: 'sanitize --action=0x04'
        parts = self.command_args.split()
        cmd = ['nvme'] + parts + [self.device_path]
        return cmd

    def _check_completion(self):
        """Check NVMe wipe completion

        Can optionally poll 'nvme sanitize-log' for actual progress.
        For now, just check if process exited.
        """
        if self.process and self.process.poll() is not None:
            return self.process.returncode == 0
        return None

    # TODO: Implement real-time progress polling via 'nvme sanitize-log'
    # def _get_sanitize_progress(self):
    #     """Query actual sanitize progress from device"""
    #     try:
    #         result = subprocess.run(
    #             ['nvme', 'sanitize-log', self.device_path, '-o', 'json'],
    #             capture_output=True, text=True, timeout=5
    #         )
    #         if result.returncode == 0:
    #             data = json.loads(result.stdout)
    #             # Parse progress from data
    #             return progress_pct
    #     except:
    #         pass
    #     return None


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

    def _estimate_duration(self):
        """Estimate SATA wipe duration

        Enhanced erase: 2-10 minutes (varies by vendor)
        Normal erase: ~1 hour per TB
        """
        if 'enhanced' in self.command_args:
            return 600  # 10 minutes for enhanced
        else:
            # Estimate based on size: 1 hour per TB
            size_tb = self.total_size / (1024**4)
            hours = max(0.5, size_tb)
            return int(hours * 3600)

    def _build_command(self):
        """Build hdparm command

        For security erase, we need to:
        1. Set password: hdparm --user-master u --security-set-pass NULL /dev/sda
        2. Erase: hdparm --user-master u --security-erase NULL /dev/sda

        We'll just build the erase command - password setting happens in run_task
        """
        # Parse: '--user-master u --security-erase-enhanced NULL'
        parts = self.command_args.split()
        cmd = ['hdparm'] + parts + [self.device_path]
        return cmd

    def _set_ata_password(self):
        """Set temporary ATA password before erase

        Returns:
            bool: True if successful
        """
        try:
            cmd = ['hdparm', '--user-master', 'u', '--security-set-pass', 'NULL', self.device_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except Exception as e:
            self.exception = f"Failed to set ATA password: {e}"
            return False

    def run_task(self):
        """Execute SATA firmware wipe (overrides base to add password step)"""
        try:
            # Step 1: Set temporary password
            if not self.opts.dry_run:
                if not self._set_ata_password():
                    self.exception = "Failed to set ATA security password"
                    self.done = True
                    return

            # Step 2: Execute erase command (base class handles this)
            super().run_task()

        except Exception:
            self.exception = traceback.format_exc()
            self.done = True
