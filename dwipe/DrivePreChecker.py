#!/usr/bin/env python3
import subprocess
import os
import json
from typing import Dict, List, Optional
from dataclasses import dataclass, field

@dataclass
class PreCheckResult:
    # Key = Short Code (Frozen, Locked), Value = Long Description
    issues: Dict[str, str] = field(default_factory=dict)
    modes: Dict[str, str] = field(default_factory=dict)

class DrivePreChecker:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout

    def check_nvme_drive(self, device: str) -> PreCheckResult:
        result = PreCheckResult()
        try:
            id_ctrl = subprocess.run(
                ['nvme', 'id-ctrl', device, '-o', 'json'],
                check=False, capture_output=True, text=True, timeout=self.timeout
            )

            if id_ctrl.returncode != 0:
                result.issues['Unresponsive'] = "NVMe controller did not respond to id-ctrl"
                return result

            data = json.loads(id_ctrl.stdout)

            # 1. Sanitize Support
            sanicap = data.get('sanicap', 0)
            if sanicap > 0:
                if sanicap & 0x04: result.modes['crypto'] = 'sanitize --action=0x04'
                if sanicap & 0x02: result.modes['hwBlock'] = 'sanitize --action=0x02'
                if sanicap & 0x08: result.modes['hwOvWr'] = 'sanitize --action=0x03'

            # 2. Format Support (Legacy)
            if 'Format NVM' in id_ctrl.stdout:
                fna = data.get('fna', 0)
                if (fna >> 2) & 0x1:
                    result.modes['fmtCrypto'] = 'format --ses=2'
                result.modes['fmtErase'] = 'format --ses=1'

            if not result.modes:
                result.issues['Unsupported'] = "Drive lacks Sanitize or Format NVM capabilities"

        except Exception as e:
            result.issues['Error'] = f"NVMe Probe Exception: {str(e)}"

        return result

    def check_ata_drive(self, device: str) -> PreCheckResult:
        result = PreCheckResult()
        try:
            info = subprocess.run(
                ['hdparm', '-I', device],
                check=False, capture_output=True, text=True, timeout=self.timeout
            )

            if info.returncode != 0:
                result.issues['Unresponsive'] = "Drive did not respond to hdparm"
                return result

            out = info.stdout.lower()

            # 1. Hardware Support Check
            if "security erase unit" not in out:
                result.issues['Unsupported'] = "Drive does not support ATA Security Erase"
                return result

            # 2. Frozen Check
            if "frozen" in out and "not frozen" not in out:
                result.issues['Frozen'] = "Drive is FROZEN (BIOS/OS lock). Cycle power or Suspend/Resume."

            # 3. Security Enabled (Password set)
            if "enabled" in out and "not enabled" not in out:
                result.issues['Locked'] = "Security is ENABLED (Drive is currently password locked)"

            # 4. Populate Modes only if no fatal issues
            if not result.issues:
                if "enhanced erase" in out:
                    result.modes['crypto'] = '--user-master u --security-erase-enhanced NULL'
                result.modes['hwErase'] = '--user-master u --security-erase NULL'

        except Exception as e:
            result.issues['Error'] = f"ATA Probe Exception: {str(e)}"

        return result