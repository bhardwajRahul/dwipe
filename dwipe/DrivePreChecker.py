#!/usr/bin/env python3
import subprocess
import json
from dataclasses import dataclass
from .SataTool import SataTool

@dataclass
class PreCheckResult:
    # Comma-separated strings of capabilities and issues
    issues: str = ""  # e.g. "Frozen, Locked"
    modes: str = ""   # e.g. "Crypto, Block, Ovwr"

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
                result.issues = "Unresponsive"
                return result

            data = json.loads(id_ctrl.stdout)

            # 1. Sanitize Support
            modes = []
            sanicap = data.get('sanicap', 0)
            if sanicap > 0:
                if sanicap & 0x04: modes.append('Crypto')
                if sanicap & 0x02: modes.append('Block')
                if sanicap & 0x01: modes.append('Ovwr')

            # 2. Format Support
            oncs = data.get('oncs', 0)
            if oncs & 0x04:  # Format NVM command supported
                fna = data.get('fna', 0)
                if fna & 0x04:
                    modes.append('FCrypto')
                modes.append('FErase')

            if modes:
                result.modes = ', '.join(modes)
            else:
                result.issues = "Unsupported"

        except Exception as e:
            result.issues = f"Error: {str(e)}"

        return result

    def check_ata_drive(self, device: str) -> PreCheckResult:
        result = PreCheckResult()
        try:
            tool = SataTool(device)
            verdict = tool.get_wipe_verdict()
            if verdict == 'OK':
                # Populate Modes only if no fatal issues
                modes = []
                secures = tool.secures
                if secures.enhanced_erase_supported:
                    modes.append('Enhanced')
                modes.append('Erase')
                result.modes = ', '.join(modes)
            elif verdict == 'DumbDevice':
                pass  # No security feature - don't report as error (e.g., USB thumb drive)
            else:
                result.issues = verdict

        except Exception as e:
            result.issues = f"Error: {str(e)}"

        return result