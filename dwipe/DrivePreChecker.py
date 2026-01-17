#!/usr/bin/env python3
import subprocess
import os
import json
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from .SataTool import SataTool

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
                if sanicap & 0x04: result.modes['CryptoNv'] = 'sanitize --action=0x04'
                if sanicap & 0x02: result.modes['BlockNv'] = 'sanitize --action=0x02'
                if sanicap & 0x08: result.modes['OvwrNv'] = 'sanitize --action=0x03'

            # 2. Format Support (Legacy)
            if 'Format NVM' in id_ctrl.stdout:
                fna = data.get('fna', 0)
                if (fna >> 2) & 0x1:
                    result.modes['FmtCryptoNv'] = 'format --ses=2'
                result.modes['FmtEraseNv'] = 'format --ses=1'

            if not result.modes:
                result.issues['Unsupported'] = "Drive lacks Sanitize or Format NVM capabilities"

        except Exception as e:
            result.issues['Error'] = f"NVMe Probe Exception: {str(e)}"

        return result

    def check_ata_drive(self, device: str) -> PreCheckResult:
        result = PreCheckResult()
        try:
            tool = SataTool(device)
            verdict = tool.get_wipe_verdict()
            if verdict == 'OK':
                # Populate Modes only if no fatal issues
                secures = tool.secures
                if secures.enhanced_erase_supported:
                    result.modes['EnhancedSd'] = '--user-master u --security-erase-enhanced NULL'
                result.modes['EraseSd'] = '--user-master u --security-erase NULL'
            elif verdict == 'DumbDevice':
                pass  # No security feature - don't report as error (e.g., USB thumb drive)
            else:
                result.issues = {verdict: verdict}

        except Exception as e:
            result.issues['Error'] = f"ATA Probe Exception: {str(e)}"

        return result