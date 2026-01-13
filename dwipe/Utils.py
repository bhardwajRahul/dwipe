"""
Utils class - Utility functions for dwipe
"""
import os
import sys
import datetime
from pathlib import Path
from .StructuredLogger import StructuredLogger


class Utils:
    """Utility functions encapsulated as a family"""

    # Singleton logger instance
    _logger = None

    @staticmethod
    def get_logger():
        """Get or create the singleton StructuredLogger instance"""
        if Utils._logger is None:
            Utils._logger = StructuredLogger(
                app_name='dwipe',
                log_dir=Utils.get_config_dir()
            )
        return Utils._logger

    @staticmethod
    def human(number):
        """Return a concise number description."""
        suffixes = ['K', 'M', 'G', 'T']
        number = float(number)
        while suffixes:
            suffix = suffixes.pop(0)
            number /= 1000  # decimal
            if number < 999.95 or not suffixes:
                return f'{number:.1f}{suffix}B'  # decimal
        return None

    @staticmethod
    def ago_str(delta_secs, signed=False):
        """Turn time differences in seconds to a compact representation;
        e.g., '18h·39m' or '450ms' for sub-second times
        """
        abs_delta = delta_secs if delta_secs >= 0 else -delta_secs

        # For sub-second times, show milliseconds
        if 0.00051 <= abs_delta < 0.99949:
            ms = int(abs_delta * 1000)
            rv = '-' if signed and delta_secs < 0 else ''
            return rv + f'{ms}ms'

        ago = int(max(0, round(abs_delta)))
        divs = (60, 60, 24, 7, 52, 9999999)
        units = ('s', 'm', 'h', 'd', 'w', 'y')
        vals = (ago % 60, int(ago / 60))  # seed with secs, mins (step til 2nd fits)
        uidx = 1  # best units
        for div in divs[1:]:
            if vals[1] < div:
                break
            vals = (vals[1] % div, int(vals[1] / div))
            uidx += 1
        rv = '-' if signed and delta_secs < 0 else ''
        rv += f'{vals[1]}{units[uidx]}' if vals[1] else ''
        rv += f'{vals[0]:d}{units[uidx - 1]}'
        return rv

    @staticmethod
    def rerun_module_as_root(module_name):
        """Rerun using the module name"""
        if os.geteuid() != 0:  # Re-run the script with sudo
            os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            vp = ['sudo', sys.executable, '-m', module_name] + sys.argv[1:]
            os.execvp('sudo', vp)

    @staticmethod
    def get_config_dir():
        """Get the dwipe config directory, handling sudo correctly

        Returns the real user's ~/.config/dwipe directory, even when running with sudo
        """
        real_user = os.environ.get('SUDO_USER')
        if real_user:
            # Running with sudo - get the real user's home directory
            import pwd
            real_home = pwd.getpwnam(real_user).pw_dir
            config_dir = Path(real_home) / '.config' / 'dwipe'
        else:
            # Not running with sudo - use normal home
            config_dir = Path.home() / '.config' / 'dwipe'

        config_dir.mkdir(parents=True, exist_ok=True)

        # Fix ownership if running with sudo
        if real_user:
            try:
                import pwd
                pw_record = pwd.getpwnam(real_user)
                uid, gid = pw_record.pw_uid, pw_record.pw_gid
                os.chown(config_dir, uid, gid)
                # Also fix parent .config directory if we created it
                parent = config_dir.parent
                if parent.exists():
                    os.chown(parent, uid, gid)
            except (OSError, KeyError):
                pass  # Ignore permission errors and missing users

        return config_dir

    @staticmethod
    def fix_file_ownership(file_path):
        """Fix file ownership to the real user when running with sudo"""
        real_user = os.environ.get('SUDO_USER')
        if real_user:
            try:
                import pwd
                pw_record = pwd.getpwnam(real_user)
                uid, gid = pw_record.pw_uid, pw_record.pw_gid
                os.chown(file_path, uid, gid)
            except (OSError, KeyError):
                pass  # Ignore permission errors and missing users

    @staticmethod
    def get_log_path():
        """Get the path to the log file, creating directory if needed"""
        log_dir = Utils.get_config_dir()
        return log_dir / 'log.txt'

    @staticmethod
    def trim_log_if_needed(log_path, max_lines=1000):
        """Trim log file by removing oldest 1/3 if it exceeds max_lines"""
        try:
            if not log_path.exists():
                return

            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if len(lines) > max_lines:
                # Keep the newest 2/3 of the log
                keep_count = len(lines) * 2 // 3
                with open(log_path, 'w', encoding='utf-8') as f:
                    f.writelines(lines[-keep_count:])
        except Exception:
            pass  # Don't fail if log trimming fails

    @staticmethod
    def get_device_dict(partitions, partition):
        """Extract device information from partition namespace as dict

        Args:
            partition: Partition namespace object with device attributes

        Returns:
            dict: Device information for structured logging
        """
        if partition.name in partitions:
            disk = partitions[partition.name]
            while disk.parent:
                disk = partitions[disk.parent]
        else:
            disk = partition
        device_dict = {
            "name": partition.name,
            "path": f"/dev/{partition.name}",
            "size": Utils.human(partition.size_bytes),
        }
        device_dict["uuid"] = partition.uuid
        if partition.type:
            device_dict["type"] = partition.type
        if partition.fstype:
            device_dict["fstype"] = partition.fstype
        if partition.label:
            device_dict["label"] = partition.label

        device_dict["model"] = disk.model
        device_dict["serial"] = disk.serial
        device_dict["port"] = disk.port

        return device_dict

    @staticmethod
    def log_wipe_structured(partitions, partition, job, mode=None):
        """Log a wipe or verify operation using structured logging

        Args:
            partition: Partition namespace object with device info
            job: WipeJob object with job statistics
            mode: Optional mode override (defaults to job.opts.wipe_mode)
        """
        logger = Utils.get_logger()

        # Determine log level based on result
        is_verify_only = getattr(job, 'is_verify_only', False)
        is_stopped = job.do_abort

        if is_verify_only:
            level = "VERIFY_STOPPED" if is_stopped else "VERIFY_COMPLETE"
        else:
            level = "WIPE_STOPPED" if is_stopped else "WIPE_COMPLETE"

        # Get the three sections
        plan = job.get_plan_dict(mode)
        device = Utils.get_device_dict(partitions, partition)
        summary = job.get_summary_dict()

        # Create summary message
        result_str = summary['result']
        size_str = device['size']
        time_str = summary['total_elapsed']
        # Get rate from first step (wipe step)
        rate_str = summary['steps'][0]['rate'] if summary['steps'] else 'N/A'

        # Build base message
        operation = plan['operation'].capitalize()
        message = f"{operation} {result_str}: {device['name']} {size_str}"

        # Add percentage if stopped
        if result_str == 'stopped' and summary.get('pct_complete', 0) > 0:
            message += f" ({summary['pct_complete']:.0f}%)"

        # Add timing and rate
        message += f" in {time_str} @ {rate_str}"

        # Add error reason if present
        abort_reason = summary.get('abort_reason')
        if abort_reason:
            message += f" [Error: {abort_reason}]"

        # Log the structured event
        logger.put(
            level,
            message,
            data={
                "plan": plan,
                "device": device,
                "summary": summary
            }
        )

    @staticmethod
    def log_wipe(device_name, size_bytes, mode, result, elapsed_time=None, uuid=None, label=None, fstype=None, pct=None, verify_result=None):
        """Log a wipe or verify operation to ~/.config/dwipe/log.txt

        Args:
            device_name: Device name (e.g., 'sdb1')
            size_bytes: Size of device in bytes
            mode: 'Rand', 'Zero', or 'Vrfy' (for verify operations)
            result: 'completed' or 'stopped'
            elapsed_time: Optional elapsed time in seconds
            uuid: Optional UUID of the partition
            label: Optional label of the partition (only for non-wiped)
            fstype: Optional filesystem type (only for non-wiped)
            pct: Optional percentage (for stopped wipes)
            verify_result: Optional verify result (chi-squared value for Rand verifies)
        """
        log_path = Utils.get_log_path()

        # Trim log if needed before appending
        Utils.trim_log_if_needed(log_path)

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        size_str = Utils.human(size_bytes)
        time_str = f' in {Utils.ago_str(int(elapsed_time))}' if elapsed_time else ''

        # Show percentage if available, otherwise 100% for completed or result status
        if result == 'completed':
            status_str = '100%'
        elif result in ('OK', 'FAIL', 'skip'):
            # For verify operations: show 100% and the result
            status_str = f'100%'
        elif pct is not None:
            status_str = f'{pct:3d}%'
        else:
            status_str = f'{result:>4s}'

        # Build UUID field (last 8 chars or full if shorter)
        uuid_str = uuid[-8:] if uuid and len(uuid) >= 8 else (uuid if uuid else '-')

        # For wiped/verified disks, don't show label/fstype (they shouldn't have any)
        # Only show for non-completed operations
        if (result in ('completed', 'OK', 'FAIL', 'skip')) and mode in ('Rand', 'Zero', 'Vrfy'):
            info_str = ''
        else:
            # Show fstype and label for non-wiped disks
            fstype_str = fstype if fstype and fstype.strip() else '-'
            label_str = f"'{label}'" if label and label.strip() else "'-'"
            info_str = f' | {fstype_str} {label_str}'

        # Add result status for verify operations (OK/FAIL)
        result_str = ''
        if result in ('OK', 'FAIL', 'skip'):
            result_str = f' | {result}'

        # Add verify result details if available (failure reason or stats)
        stats_str = ''
        if verify_result:
            stats_str = f' | {verify_result}'

        log_line = f'{timestamp} | {mode:4s} | {status_str} {size_str:>8s}{time_str:>12s} | {device_name:8s} | {uuid_str:8s}{info_str}{result_str}{stats_str}\n'

        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(log_line)
            # Fix ownership if running with sudo
            Utils.fix_file_ownership(log_path)
        except Exception:
            pass  # Don't fail if logging fails
