import os
import json
import subprocess
import re
import time
from types import SimpleNamespace

class NvmeTool:
    """NVMe equivalent of SataTool using nvme-cli."""
    
    def __init__(self, device_name, timeout=10):
        self.timeout = timeout
        # Handle /dev/nvme0n1 or nvme0n1
        if device_name.startswith('/dev/'):
            self.device_name = device_name[len('/dev/'):]
            self.device_path = device_name
        else:
            self.device_name = device_name
            self.device_path = f'/dev/{self.device_name}'
        
        self.caps = None  # Stores parsed sanitize/format capabilities
        self.job = SimpleNamespace()
        self.job.process = None
        self.job.wipe_started_mono = None
        self.job.est_secs = 60 # NVMe is usually much faster than SATA
        
    def run_nvme_cmd(self, args, json_out=True):
        """Helper to run nvme-cli commands."""
        cmd = ['nvme'] + args
        if json_out:
            cmd += ['-o', 'json']
        
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
            if json_out and res.stdout:
                return json.loads(res.stdout)
            return res
        except Exception as e:
            return None

    def refresh_capabilities(self):
        """
        Detects if Sanitize and Format (Crypto/Erase) are supported.
        NVMe Sanitize: Check 'id-ctrl' for 'sanicap'
        NVMe Format: Check 'id-ctrl' for 'fna'
        """
        data = self.run_nvme_cmd(['id-ctrl', self.device_path])
        caps = SimpleNamespace(
            has_sanitize=False,
            crypto_erase_supported=False,
            block_erase_supported=False,
            overwrite_supported=False,
            format_crypto_supported=False,
            raw_data=data
        )

        if data:
            # Sanitize Capabilities (sanicap)
            # Bit 0: Overwrite, Bit 1: Block Erase, Bit 2: Crypto Erase
            sanicap = data.get('sanicap', 0)
            caps.has_sanitize = sanicap > 0
            caps.overwrite_supported = bool(sanicap & 0x01)
            caps.block_erase_supported = bool(sanicap & 0x02)
            caps.crypto_erase_supported = bool(sanicap & 0x04)

            # Format Capabilities (fna)
            # Bit 2 indicates if Crypto Erase is supported via Format command
            fna = data.get('fna', 0)
            caps.format_crypto_supported = bool(fna & 0x04)

        self.caps = caps
        return caps

    def get_wipe_verdict(self):
        """Determines if the drive can be wiped."""
        if not self.caps:
            self.refresh_capabilities()
        
        if not self.caps.has_sanitize and not self.caps.format_crypto_supported:
            return "Unsupported"
        
        # NVMe drives don't 'freeze' like SATA, but they can be Read-Only
        # or have Namespace management locks. 
        return "OK"

    def start_wipe(self, method='sanitize_block'):
        """
        Executes the wipe. 
        Methods: 'sanitize_block', 'sanitize_crypto', 'format_erase'
        """
        if method == 'sanitize_block':
            cmd = ['nvme', 'sanitize', '-a', 'start-block-erase', self.device_path]
        elif method == 'sanitize_crypto':
            cmd = ['nvme', 'sanitize', '-a', 'start-crypto-erase', self.device_path]
        else: # Default to standard format
            cmd = ['nvme', 'format', '-e', '1', self.device_path]

        self.job.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.job.wipe_started_mono = time.monotonic()
        return self.job.process

    def get_sanitize_status(self):
        """
        NVMe Sanitize runs in the background. 
        We must poll 'sanitize-log' to see if it's actually done.
        """
        log = self.run_nvme_cmd(['sanitize-log', self.device_path])
        if log:
            # sstat: 0=Idle, 1=In Progress, 2=Success
            status = log.get('sstat', 0) & 0x7 
            progress = log.get('sprog', 0) # 65535 = 100%
            percent = (progress / 65535.0) * 100
            return status, percent
        return None, 0

