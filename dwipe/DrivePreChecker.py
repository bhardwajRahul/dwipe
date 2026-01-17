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
                if sanicap & 0x04: result.modes['Crypto'] = 'sanitize_crypto'
                if sanicap & 0x02: result.modes['Block'] = 'sanitize_block'
                if sanicap & 0x01: result.modes['Ovwr'] = 'sanitize_overwrite'

            # 2. Format Support
            oncs = data.get('oncs', 0)
            if oncs & 0x04:  # Format NVM command supported
                fna = data.get('fna', 0)
                if fna & 0x04:
                    result.modes['FCrypto'] = 'format_crypto'
                result.modes['FErase'] = 'format_erase'

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
                    result.modes['Enhanced'] = 'enhanced'
                result.modes['Erase'] = 'normal'
            elif verdict == 'DumbDevice':
                pass  # No security feature - don't report as error (e.g., USB thumb drive)
            else:
                result.issues = {verdict: verdict}

        except Exception as e:
            result.issues['Error'] = f"ATA Probe Exception: {str(e)}"

        return result