#!/usr/bin/env python3
"""
# Drive Pre-Checks for dwipe for HW wipes
"""

import subprocess
import os
import json
from typing import List, Optional
from enum import Enum
from dataclasses import dataclass

# ============================================================================
# Part 2: Drive Pre-Checks
# ============================================================================

@dataclass
class PreCheckResult:
    issues: List[str] = None  # list of "why not" ... any set, no wipe
    modes = {}
    
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
                    timeout=5, check=False
                )
                if udev_info.returncode == 0 and 'ID_BUS=usb' in udev_info.stdout:
                    return True
            except Exception:
                pass
        return False

    def check_nvme_drive(self, device: str) -> PreCheckResult:
        """Probes NVMe and returns specific command flags for available wipe modes"""
        result = PreCheckResult()
        result.modes = {}
        
        try:
            # Get controller capabilities in JSON for easy parsing
            id_ctrl = subprocess.run(
                ['nvme', 'id-ctrl', device, '-o', 'json'], check=False,
                capture_output=True, text=True, timeout=self.timeout
            )
            
            if id_ctrl.returncode != 0:
                result.issues.append("NVMe controller unresponsive")
                return result

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
                ['hdparm', '-I', device], check=False,
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
    
