#!/usr/bin/env python3
"""
Hardware Secure Erase Module for dwipe
Provides pre-checks, execution, monitoring, and fallback for hardware-level wipes
"""

import subprocess
import shutil
import os
import time
import threading
import sys
from typing import Dict, List, Optional, Tuple, Callable
from enum import Enum
from dataclasses import dataclass

# ============================================================================
# Part 1: Tool Manager (Dependency Management)
# ============================================================================

# ============================================================================
# Part 2: Drive Pre-Checks
# ============================================================================

class EraseStatus(Enum):
    NOT_STARTED = "not_started"
    STARTING = "starting"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"
    UNKNOWN = "unknown"

@dataclass
class OLD-PreCheckResult:
    compatible: bool = False
    tool: Optional[str] = None
    frozen: bool = False
    locked: bool = False
    enhanced_supported: bool = False
    issues: List[str] = None
    recommendation: Optional[str] = None

@dataclass
class PreCheckResult:
#   compatible: bool = False
#   tool: Optional[str] = None
#   frozen: bool = False
#   locked: bool = False
#   enhanced_supported: bool = False
    issues: List[str] = None  # list of "why not" ... any set, no wipe
#   recommendation: Optional[str] = None
    modes = {}   # dict of descr/how  'Cropto': '--wipe=crypto'
    
    def __post_init__(self):
        if self.issues is None:
            self.issues = []

class DrivePreChecker:
    """Pre-check drive before attempting secure erase"""
    
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
    
    def is_usb_attached(self, device: str) -> bool:
        """Check if device is USB-attached"""
        dev_name = os.path.basename(device)
        
        # Check via sysfs
        sys_path = f'/sys/block/{dev_name}'
        if os.path.exists(sys_path):
            try:
                # Check if in USB hierarchy
                real_path = os.path.realpath(sys_path)
                if 'usb' in real_path.lower():
                    return True
                
                # Check via udev
                udev_info = subprocess.run(
                    ['udevadm', 'info', '-q', 'property', '-n', device],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if udev_info.returncode == 0 and 'ID_BUS=usb' in udev_info.stdout:
                    return True
            except:
                pass
        
        return False

    def check_nvme_drive(self, device: str) -> PreCheckResult:
        """Probes NVMe and returns specific command flags for available wipe modes"""
        result = PreCheckResult()
        result.modes = {}
        
        try:
            # Get controller capabilities in JSON for easy parsing
            id_ctrl = subprocess.run(
                ['nvme', 'id-ctrl', device, '-o', 'json'],
                capture_output=True, text=True, timeout=self.timeout
            )
            
            if id_ctrl.returncode != 0:
                result.issues.append("NVMe controller unresponsive")
                return result

            import json
            data = json.loads(id_ctrl.stdout)
            
            # 1. Check for Sanitize Capabilities (The most modern/safe method)
            # Bit 1: Block, Bit 2: Crypto, Bit 3: Overwrite
            sanicap = data.get('sanicap', 0)
            if sanicap > 0:
                # We use OrderedDict or similar to put the 'best' options first
                if sanicap & 0x04: # Crypto Erase
                    result.modes['Sanitize-Crypto'] = 'sanitize --action=0x04'
                if sanicap & 0x02: # Block Erase (Physical)
                    result.modes['Sanitize-Block'] = 'sanitize --action=0x02'
                if sanicap & 0x08: # Overwrite
                    result.modes['Sanitize-Overwrite'] = 'sanitize --action=0x03'

            # 2. Check for Legacy Format Capabilities
            # Bit 1: Crypto, Bit 2: User Data Erase
            fna = data.get('fna', 0)
            if 'Format NVM' in id_ctrl.stdout:
                # Check if Crypto Erase is supported via Format
                if (fna >> 2) & 0x1:
                    result.modes['Format-Crypto'] = 'format --ses=2'
                # Standard User Data Erase
                result.modes['Format-Erase'] = 'format --ses=1'

            # Final Validation
            if not result.modes:
                result.issues.append("No HW wipe modes (Sanitize/Format) supported")

        except Exception as e:
            result.issues.append(f"Probe Error: {str(e)}")
        
        return result

    def check_ata_drive(self, device: str) -> PreCheckResult:
        """Probes SATA/ATA and returns hdparm flags or specific blocking reasons
          + Why the "NULL" password? In the modes dictionary above, we use NULL.
            - To perform an ATA Secure Erase, you have to set a temporary password first,
              then immediately issue the erase command with that same password.
            - Most tools (and hdparm itself) use NULL or a simple string like p
              as a throwaway.
            - Note: If the dwipe app crashes after setting the password but before the
              erase finishes, the drive will stay locked. On the next run, your enabled
              check (Step 3) will catch this.
          + Handling "Frozen" in the UI
            -the "Frozen" issue is the one that will frustrate users most.
            -The "Short Crisp Reason": Drive is FROZEN.
            - The Fix: To unfreeze, try suspending (sleeping) and waking the computer,
              or re-plugging the drive's power cable."

          + Now, dwipe builds that list:
            It calls can_use_hardware_erase().
            It looks at result.issues. If empty, the [f]:irmW key is active.
        """
        result = PreCheckResult()
        result.modes = {}
        
        try:
            # Get drive info via hdparm
            info = subprocess.run(
                ['hdparm', '-I', device],
                capture_output=True, text=True, timeout=self.timeout
            )
            
            if info.returncode != 0:
                result.issues.append("Drive not responsive to hdparm")
                return result

            out = info.stdout.lower()

            # 1. Check if the drive even supports Security Erase
            if "security erase unit" not in out:
                result.issues.append("Drive does not support ATA Security Erase")
                return result

            # 2. Check for "Frozen" state (The most common blocker)
            # A frozen drive rejects security commands until a power cycle.
            if "frozen" in out and "not frozen" not in out:
                result.issues.append("Drive is FROZEN (BIOS/OS lock)")
                # You might want to keep this in issues so user can't select it,
                # or move it to a 'warning' if you want to allow them to try anyway.

            # 3. Check if security is already "Enabled" (Drive is locked)
            if "enabled" in out and "not enabled" not in out:
                # If it's already locked, we can't wipe without the existing password.
                result.issues.append("Security is ENABLED (Drive is password locked)")

            # 4. Populate Modes if no fatal issues
            if not result.issues:
                # Enhanced Erase: Usually writes a pattern or destroys encryption keys
                if "enhanced erase" in out:
                    result.modes['ATA-Enhanced'] = '--user-master u --security-erase-enhanced NULL'
                
                # Normal Erase: Usually writes zeros to the whole platter
                result.modes['ATA-Normal'] = '--user-master u --security-erase NULL'

        except Exception as e:
            result.issues.append(f"ATA Probe Error: {str(e)}")
        return result
    

# ============================================================================
# Part 3: Drive Eraser with Monitoring
# ============================================================================

class DriveEraser:
    """Execute and monitor hardware secure erase"""
    
    def __init__(self, progress_callback: Optional[Callable] = None):
        self.status = EraseStatus.NOT_STARTED
        self.start_time = None
        self.progress_callback = progress_callback
        self.monitor_thread = None
        self.current_process = None
    
    def run_firmware_wipe(self):
        """The thread target for firmware wipes"""
        self.start_mono = time.monotonic()
        
        # 1. Start the process (non-blocking)
        # self.opts.hw_cmd might be: "nvme sanitize --action=0x02"
        full_cmd = f"{self.opts.tool} {self.opts.hw_cmd} {self.device_path}"
        self.process = subprocess.Popen(full_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # 2. Polling Loop
        while not self._abort_requested:
            if self.process.poll() is not None:
                break
                
            # Optional: For NVMe, you can poll 'nvme sanitize-log' here 
            # to get actual 0-100% progress and update self.total_written
            
            time.sleep(1) 

        # 3. Finalize
        self.finish_mono = time.monotonic()
        if self.process.returncode == 0:
            self.total_written = self.total_size  # Mark as done for the UI
    
    def abort(self):
        self._abort_requested = True
        if self.process and self.process.poll() is None:
            self.process.terminate() # Try nice first
            time.sleep(0.5)
            self.process.kill()      # Then hammer it
    
    """
    # Inside get_summary_dict...
    is_hw = getattr(self.opts, 'is_hardware', False)

    if is_hw:
        # Rate and written bytes don't follow standard rules
        wipe_step["rate"] = "Hardware"
        wipe_step["status"] = "Sanitizing..." if not self.done else "Complete"
    
    """
        
    def start_nvme_erase(self, device: str) -> bool:
        """Start NVMe secure erase (non-blocking)"""
        try:
            self.current_process = subprocess.Popen(
                ['nvme', 'format', device, '--ses=1'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.status = EraseStatus.STARTING
            self.start_time = time.time()
            self._start_monitoring(device, 'nvme')
            return True
            
        except Exception as e:
            print(f"Failed to start NVMe erase: {e}")
            self.status = EraseStatus.FAILED
            return False
    
    def start_ata_erase(self, device: str, enhanced: bool = True) -> bool:
        """Start ATA secure erase (non-blocking)"""
        try:
            # Build command
            cmd = ['hdparm', '--user-master', 'u']
            if enhanced:
                cmd.extend(['--security-erase-enhanced', 'NULL'])
            else:
                cmd.extend(['--security-erase', 'NULL'])
            cmd.append(device)
            
            self.current_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.status = EraseStatus.STARTING
            self.start_time = time.time()
            self._start_monitoring(device, 'ata')
            return True
            
        except Exception as e:
            print(f"Failed to start ATA erase: {e}")
            self.status = EraseStatus.FAILED
            return False
    
    def _start_monitoring(self, device: str, drive_type: str):
        """Start background monitoring thread"""
        def monitor():
            time.sleep(3)  # Let command start
            self.status = EraseStatus.IN_PROGRESS
            
            check_interval = 5
            max_checks = 7200  # 10 hours max
            
            for _ in range(max_checks):
                # Check if process completed
                if self.current_process and self.current_process.poll() is not None:
                    if self.current_process.returncode == 0:
                        self.status = EraseStatus.COMPLETE
                    else:
                        self.status = EraseStatus.FAILED
                    break
                
                # Update progress callback
                if self.progress_callback:
                    elapsed = time.time() - self.start_time
                    progress = self._estimate_progress(elapsed, drive_type)
                    self.progress_callback(progress, elapsed, self.status)
                
                time.sleep(check_interval)
            else:
                self.status = EraseStatus.FAILED
        
        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()
    
    def _estimate_progress(self, elapsed_seconds: float, drive_type: str) -> float:
        """Estimate fake progress based on typical times"""
        if drive_type == 'nvme':
            progress = min(1.0, elapsed_seconds / 30)
        elif drive_type == 'ata':
            # Very rough estimate - would need drive size for better guess
            progress = min(1.0, elapsed_seconds / 3600)
        else:
            progress = 0.0
        
        return progress * 100
    
    def get_status(self) -> Dict:
        """Get current status info"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        return {
            'status': self.status.value,
            'elapsed_seconds': elapsed,
            'monitor_alive': self.monitor_thread and self.monitor_thread.is_alive(),
            'process_active': self.current_process and self.current_process.poll() is None
        }
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for erase to complete"""
        if not self.current_process:
            return False
        
        try:
            return_code = self.current_process.wait(timeout=timeout)
            return return_code == 0
        except subprocess.TimeoutExpired:
            return False

# ============================================================================
# Part 4: Main Wipe Controller (Integration Point)
# ============================================================================

class HardwareWipeController:
    """
    Main controller for hardware wiping.
    This is what you'd integrate into dwipe.
    """
    
    def __init__(self, auto_install_tools: bool = False, verbose: bool = False):
        self.tool_mgr = ToolManager(auto_install=auto_install_tools, verbose=verbose)
        self.pre_checker = DrivePreChecker(timeout=15)
        self.eraser = None
        self.verbose = verbose
    
    def _log(self, message: str):
        if self.verbose:
            print(f"[HardwareWipe] {message}")
    
    def prepare(self) -> bool:
        """Ensure required tools are available"""
        if not self.tool_mgr.ensure_tool('hdparm', critical=True):
            return False
        if not self.tool_mgr.ensure_tool('nvme', critical=True):
            return False
        return True
    
    def pre_check(self, device: str) -> PreCheckResult:
        """Perform comprehensive pre-check"""
        self._log(f"Pre-checking {device}...")
        result = self.pre_checker.can_use_hardware_erase(device)
        
        if self.verbose:
            print(f"Pre-check for {device}:")
            print(f"  Compatible: {result.compatible}")
            print(f"  Tool: {result.tool}")
            if result.issues:
                print(f"  Issues: {', '.join(result.issues)}")
            if result.recommendation:
                print(f"  Recommendation: {result.recommendation}")
        
        return result
    
    def wipe(self, device: str, fallback_callback: Optional[Callable] = None) -> bool:
        """
        Execute hardware wipe with automatic fallback.
        
        Args:
            device: Device path (/dev/sda, /dev/nvme0n1, etc.)
            fallback_callback: Function to call if hardware wipe fails
                               Should accept device path and return bool
        
        Returns:
            True if wipe succeeded (hardware or software), False otherwise
        """
        if not self.prepare():
            print("Required tools not available")
            return False
        
        # Step 1: Pre-check
        pre_check = self.pre_check(device)
        
        if not pre_check.compatible:
            print(f"Hardware erase not compatible for {device}:")
            for issue in pre_check.issues:
                print(f"  - {issue}")
            
            if fallback_callback:
                self._log("Falling back to software wipe...")
                return fallback_callback(device)
            return False
        
        # Step 2: Show user what to expect
        tool_name = pre_check.tool
        print(f"Using {tool_name} for hardware secure erase...")
        print("Note: Drive erases in firmware - tool will exit immediately.")
        
        if tool_name == 'nvme':
            print("Expected time: 2-10 seconds")
        elif tool_name == 'hdparm' and pre_check.enhanced_supported:
            print("Expected time: 10-60 seconds (enhanced erase)")
        elif tool_name == 'hdparm':
            print("Expected time: 1-3 hours per TB (normal erase)")
        
        # Step 3: Start erase
        self.eraser = DriveEraser(progress_callback=self._progress_update)
        
        try:
            if tool_name == 'nvme':
                success = self.eraser.start_nvme_erase(device)
            else:  # hdparm
                enhanced = pre_check.enhanced_supported
                success = self.eraser.start_ata_erase(device, enhanced)
            
            if not success:
                raise RuntimeError("Failed to start erase")
            
            # Step 4: Monitor with timeout
            timeout = self._get_timeout(tool_name, device)
            print(f"Waiting up to {timeout//60} minutes for completion...")
            
            # Simple spinner while waiting
            spinner = ['|', '/', '-', '\\']
            i = 0
            
            while True:
                status = self.eraser.get_status()
                
                if status['status'] == EraseStatus.COMPLETE.value:
                    print(f"\nHardware secure erase completed successfully!")
                    return True
                
                elif status['status'] == EraseStatus.FAILED.value:
                    print(f"\nHardware secure erase failed")
                    break
                
                # Show spinner and elapsed time
                elapsed = status['elapsed_seconds']
                print(f"\r{spinner[i % 4]} Erasing... {int(elapsed)}s elapsed", end='')
                i += 1
                
                # Check timeout
                if elapsed > timeout:
                    print(f"\nTimeout after {timeout} seconds")
                    break
                
                time.sleep(0.5)
            
            # If we get here, hardware failed
            if fallback_callback:
                print("Falling back to software wipe...")
                return fallback_callback(device)
            
            return False
            
        except Exception as e:
            print(f"Error during hardware erase: {e}")
            if fallback_callback:
                return fallback_callback(device)
            return False
    
    def _progress_update(self, progress: float, elapsed: float, status: EraseStatus):
        """Callback for progress updates"""
        if self.verbose:
            print(f"[Progress] {progress:.1f}% - {elapsed:.0f}s - {status.value}")
    
    def _get_timeout(self, tool: str, device: str) -> int:
        """Get appropriate timeout based on drive type"""
        if tool == 'nvme':
            return 30  # 30 seconds for NVMe
        elif tool == 'hdparm':
            # Try to get drive size for better timeout
            try:
                size_gb = self._get_drive_size_gb(device)
                # 2 hours per TB, minimum 30 minutes
                hours = max(0.5, (size_gb / 1024) * 2)
                return int(hours * 3600)
            except:
                return 7200  # 2 hours default
        return 3600  # 1 hour default
    
    def _get_drive_size_gb(self, device: str) -> float:
        """Get drive size in GB"""
        try:
            # Use blockdev to get size
            result = subprocess.run(
                ['blockdev', '--getsize64', device],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                size_bytes = int(result.stdout.strip())
                return size_bytes / (1024**3)  # Convert to GB
        except:
            pass
        return 500  # Default guess
    
    def apply_marker(self):
    # 1. Force the OS to realize the partitions are gone
    subprocess.run(['blockdev', '--rereadpt', self.device_path])
    time.sleep(1) # Give the kernel a breath
    
    try:
        with open(self.device_path, 'wb') as f:
            # Clear first 16K
            f.write(b'\x00' * 16384)
            # Seek to 15K
            f.seek(15360)
            f.write(self.generate_json_marker())
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        # If this happens, the drive is likely still 'settling' its FTL
        return "RETRY_NEEDED"

# ============================================================================
# Part 5: Example Usage & Integration Helper
# ============================================================================

def example_software_wipe(device: str) -> bool:
    """Example fallback function for software wipe"""
    print(f"[Software] Would wipe {device} with dd/scrub/etc.")
    # Implement your existing software wipe here
    return True

def main():
    """Example standalone usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Hardware Secure Erase Test')
    parser.add_argument('device', help='Device to wipe (e.g., /dev/sda)')
    parser.add_argument('--auto-install', action='store_true', 
                       help='Automatically install missing tools')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--no-fallback', action='store_true',
                       help='Don\'t fall back to software wipe')
    args = parser.parse_args()
    
    # Create controller
    controller = HardwareWipeController(
        auto_install_tools=args.auto_install,
        verbose=args.verbose
    )
    
    # Define fallback
    fallback = None if args.no_fallback else example_software_wipe
    
    # Execute wipe
    success = controller.wipe(args.device, fallback_callback=fallback)
    
    if success:
        print(f"\n✓ Wipe completed successfully")
        return 0
    else:
        print(f"\n✗ Wipe failed")
        return 1

if __name__ == '__main__':
    sys.exit(main())