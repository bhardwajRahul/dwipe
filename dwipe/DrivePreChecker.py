#!/usr/bin/env python3
import subprocess
import json
from dataclasses import dataclass
from .SataTool import SataTool

@dataclass
class PreCheckResult:
    # Dict of capabilities and issues
    issues: dict = None  # e.g. {"Frozen": reason, "Locked": reason}
    modes: dict = None   # e.g. {"Crypto": ..., "Block": ..., "Ovwr": ...}

    def __post_init__(self):
        if self.issues is None:
            self.issues = {}
        if self.modes is None:
            self.modes = {}

class DrivePreChecker:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout

    @staticmethod
    def get_wipe_command_args(wipe_type: str) -> str:
        """Get command arguments for a given wipe type name.

        Maps wipe mode names to their corresponding firmware command arguments.
        This is a static mapping independent of device capabilities.

        Args:
            wipe_type: Wipe mode name (e.g., 'Crypto', 'Block', 'Enhanced', 'Erase')

        Returns:
            str: Command argument string (e.g., 'sanitize_crypto', 'enhanced')
                 Empty string if wipe_type is unknown
        """
        wipe_args_map = {
            # NVMe sanitize operations
            'Crypto': 'sanitize_crypto',
            'Block': 'sanitize_block',
            'Ovwr': 'sanitize_overwrite',
            # NVMe format operations
            'FCrypto': 'format_crypto',
            'FErase': 'format_erase',
            # SATA/ATA security erase operations
            'Enhanced': 'enhanced',
            'Erase': 'normal',
        }
        return wipe_args_map.get(wipe_type, '')

    def check_nvme_drive(self, device: str) -> PreCheckResult:
        result = PreCheckResult()
        try:
            id_ctrl = subprocess.run(
                ['nvme', 'id-ctrl', device, '-o', 'json'],
                check=False, capture_output=True, text=True, timeout=self.timeout
            )

            if id_ctrl.returncode != 0:
                result.issues['Unresponsive'] = "NVMe controller did not respond"
                return result

            data = json.loads(id_ctrl.stdout)

            # 1. Sanitize Support
            sanicap = data.get('sanicap', 0)
            if sanicap > 0:
                if sanicap & 0x04:
                    result.modes['Crypto'] = 'sanitize_crypto'
                if sanicap & 0x02:
                    result.modes['Block'] = 'sanitize_block'
                if sanicap & 0x01:
                    result.modes['Ovwr'] = 'sanitize_overwrite'

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
                result.issues[verdict] = verdict

        except Exception as e:
            result.issues['Error'] = f"ATA Probe Exception: {str(e)}"

        return result
