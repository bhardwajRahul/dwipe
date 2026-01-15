#!/usr/bin/env python3
""" TBD """
# pylint: disable=invalid-name,multiple-statements,line-too-long

import os
import sys
import subprocess
import re
import time
from types import SimpleNamespace
import signal

def signal_handler(sig, frame):
    print("\n" + "!" * 60)
    print("CRITICAL WARNING: INTERRUPT DETECTED")
    print("A firmware wipe is a hardware-level command.")
    print("If you kill this process, the drive will remain LOCKED.")
    print("To recover, you must run this tool again and use the 'Rescue' option.")
    print("!" * 60)
    
    # Optional: Logic to decide whether to actually exit
    confirm = input("\nReally abort and leave the drive locked? (y/N): ")
    if confirm.lower() == 'y':
        print("Aborting. Please note the drive is now in a Security Locked state.")
        sys.exit(1)
    else:
        print("Resuming monitoring...")


class SataTool:
    """ TBD """
    def __init__(self, device_name, timeout=10):
        self.timeout = timeout
        if device_name.startswith('/dev/'):
            self.device_name = device_name[len('/dev/'):]
            self.device_path = device_name
        else:
            self.device_name = device_name
            self.device_path = f'/dev/{self.device_name}'
        self.secures = None  # last parse result of security section values
        self.job = SimpleNamespace() # data about running wipe job
        self.job.process = None # set when wipe job is running
        self.job.wipe_started_mono = None # set when wipe job started
        self.job.est_secs = 0

    def run_cmd(self, argv, timeout=None):
        """ RUN a command return the output """
        timeout = self.timeout if timeout is None else timeout
        cmd = ['hdparm']
        cmd += argv if isinstance(argv, (list, tuple)) else [argv]
        cmd += [self.device_path]
        output = subprocess.run(cmd, check=False, capture_output=True,
                              text=True, timeout=timeout)
        return output

    @staticmethod
    def _make_ns():
        return SimpleNamespace(supported=False, enabled=False,
                frozen=False, locked=False, expired=False,
                enhanced_erase_supported=False, erase_est_secs=[4*60*60])

    def _parse_output_to_secures(self, output):
        secures = self._make_ns()
        self.secures = secures
        # Look specifically for the security block
        security_match = re.search(r"(?i)Security:.*?(?=\n\w|\Z)", output, re.DOTALL)
        if not security_match:
            return secures

        sec_block = security_match.group(0)

        # Better logic: Check if the keyword exists AND is not preceded by 'not'
        secures.supported = "supported" in sec_block and "not supported" not in sec_block
        secures.enabled = bool(re.search(r"(?<!not)\s+enabled", sec_block))
        secures.locked = bool(re.search(r"(?<!not)\s+locked", sec_block))
        secures.frozen = bool(re.search(r"(?<!not)\s+frozen", sec_block))
        secures.expired = bool(re.search(r"(?<!not)\s+expired", sec_block))
        secures.enhanced_erase_supported = "supported: enhanced erase" in sec_block

        minutes = re.findall(r"(\d+)min", sec_block)
        if minutes:
            secures.erase_est_secs = [60*int(m) for m in minutes]

        return secures

    def refresh_secures(self):
        """ Run a command to get the security values from hdparm """
        output = self.run_cmd('-I')
        secures = self._parse_output_to_secures(output.stdout)
        return secures

    # Logic for your App's Verdict
    def get_wipe_verdict(self): # , override_usb=False):
        """ TBD """

#       if data["bridge"]["vendor_id"] and not data["bridge"]["whitelisted"] and not override_usb:
#           return "FwErr: USB-Disallowed"
        if not self.secures:
            self.refresh_secures()

        if not self.secures.supported:
            return "WipeUnsupported"
        if self.secures.frozen:
            return "Frozen"
        if self.secures.expired:
            return "SecurityExpired"
        if self.secures.locked:
            return "Locked" # Already locked with unknown pass
        return "OK"

    def start_async_wipe(self, use_enhanced=False):
        """Starts the wipe and returns the process object immediately.
        # --- Monitoring Loop in your Main App ---
        # wipe_proc = tool.start_async_wipe()
        # while wipe_proc.poll() is None:
        #     print("Wipe in progress... (Drive is busy)")
        #     time.sleep(30)
        #
        # out, err = wipe_proc.communicate()
        """

        erase_flag = '--security-erase-enhanced' if use_enhanced else '--security-erase'
        cmd = ['hdparm', '--user-master', 'u', erase_flag, 'NULL', self.device_path]

        # Popen does NOT block. It returns immediately.
        self.job.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.job.wipe_started_mono = time.monotonic()
        idx = 0 if use_enhanced else -1
        self.job.est_secs = self.secures.erase_est_secs[idx]
        return self.job.process


    def start_wipe(self, use_enhanced=False, password="NULL", db=False):
        """
        Executes the two-stage ATA Security Erase process.
        Returns (success_bool, message)
        """
        # 1. Pre-flight check: Re-verify not frozen
        self.refresh_secures()
        verdict = self.get_wipe_verdict()
        if verdict != "OK":
            return False, f'Drive not in OK state [{verdict}]'

        # 2. Set the Security Password
        # This moves the drive from 'not enabled' to 'enabled'
        set_pass_argv = ['--user-master', 'u', '--security-set-pass', password]
        if db: print(f"Setting security password for {self.device_name}...")
        res = self.run_cmd(set_pass_argv)

        if res.returncode != 0:
            return False, f"Failed to set password: {res.stderr.strip()}"

        # 3. Verify the drive is now 'enabled'
        self.refresh_secures()
        if not self.secures.enabled:
            return False, "Password command accepted but drive is not 'enabled'."

        # NOTE: This command will run until finished or timeout.
        if db:
            self.job.process = self.start_async_wipe(use_enhanced=use_enhanced)
            return self.job

        else:
            erase_flag = '--security-erase-enhanced' if use_enhanced else '--security-erase'
            return ['hdparm', '--user-master', 'u', erase_flag, 'NULL', self.device_path]



    def rescue_locked_drive(self, password="NULL", db=False):
        """Attempts to unlock and disable security on a bricked drive."""
        if db: print(f"Attempting rescue on locked device {self.device_name}...")

        # 1. Unlock
        self.run_cmd(['--user-master', 'u', '--security-unlock', password])

        # 2. Disable (removes the 'enabled' requirement)
        self.run_cmd(['--user-master', 'u', '--security-disable', password])

        # 3. Verify
        self.refresh_secures()
        if not self.secures.locked and not self.secures.enabled:
            return True, "Drive successfully rescued and unlocked."
        return False, "Rescue failed. Password may be incorrect or drive is 'Expired'."

    def verify_wipe_result(self):
        """
        Runs AFTER start_wipe finishes to see if the drive
        actually cleared its security state.
        """
        # Refresh the namespace
        self.refresh_secures()

        # If the wipe worked, the drive automatically disables
        # the security password and clears the 'enabled' bit.
        if not self.secures.enabled and not self.secures.locked:
            return True, "Wipe Verified: Drive is unlocked and clean."

        if self.secures.locked:
            return False, "Wipe Failed: Drive is still LOCKED (Password 'NULL' still active)."

        return False, "Wipe Incomplete: Security is still enabled."
        
    def test_and_restore_block(self, offset=0):
        """
        Reads, Writes a test pattern, Reads, and Restores the original data.
        Returns (success_bool, message)
        """
        original_data = None
        test_pattern = b"WIPE_VERIFY_PATTERN_" + os.urandom(8)
        test_pattern = test_pattern.ljust(512, b'\x00')

        try:
            # Step 1: Read original
            with open(self.device_path, 'rb') as f:
                f.seek(offset)
                original_data = f.read(512)
            
            if len(original_data) != 512:
                return False, "Could not read full 512 bytes for backup."

            # Step 2: Write Test Pattern
            # Using O_SYNC to ensure it hits the controller
            fd = os.open(self.device_path, os.O_WRONLY | os.O_SYNC)
            try:
                os.lseek(fd, offset, os.SEEK_SET)
                os.write(fd, test_pattern)
                os.fsync(fd)
            finally:
                os.close(fd)

            # Step 3: Clear Cache and Verify Write
            subprocess.run(['blockdev', '--flushbufs', self.device_path], check=True)
            
            with open(self.device_path, 'rb') as f:
                f.seek(offset)
                check_write = f.read(512)
            
            if check_write != test_pattern:
                return False, "Verification failed: Test pattern was not correctly written."

            # Step 4: Restore original data
            fd = os.open(self.device_path, os.O_WRONLY | os.O_SYNC)
            try:
                os.lseek(fd, offset, os.SEEK_SET)
                os.write(fd, original_data)
                os.fsync(fd)
            finally:
                os.close(fd)

            # Step 5: Final Verification of Restoration
            subprocess.run(['blockdev', '--flushbufs', self.device_path], check=True)
            
            with open(self.device_path, 'rb') as f:
                f.seek(offset)
                final_check = f.read(512)

            if final_check == original_data:
                return True, f"Full I/O transaction successful at offset {offset}."
            else:
                return False, "CRITICAL: Failed to restore original data correctly!"

        except Exception as e:
            return False, f"I/O Transaction error: {str(e)}"
    
    def finalize_verification(self):
        """ Orchestrates the wait and the I/O test. """
        print("Wipe command finished. Entering verification phase...")
        
        # 1. Wait for hdparm to see the drive again
        ready = False
        for i in range(20): # 100 seconds total
            if self.is_ready_for_marker(): # The method we defined earlier
                ready = True
                break
            print(f"[{i*5}s] Drive still busy or re-initializing...")
            time.sleep(5)
            
        if not ready:
            return False, "Timed out waiting for drive to become ready."

        # 2. Perform the Read-Write-Restore test at the start and end
        # We test Sector 0 (Start) and a block near the end
        ok, msg = self.test_and_restore_block(offset=0)
        if not ok: return False, msg
        
        return True, "Wipe verified and I/O integrity confirmed."

def main():
    """ TBD"""
    # pylint: disable=import-outside-toplevel
    import glob
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--wipe', action='store_true',
                        help='firmware wipe of given disk')
    parser.add_argument('devs', nargs='*', help="list if sdX")
    opts = parser.parse_args()


    if not opts.wipe and not opts.devs:
        pattern = '/dev/sd?'
        opts.devs = glob.glob(pattern)
    for dev in opts.devs:
        tool = SataTool(os.path.basename(dev))
        secures = tool.refresh_secures()
        verdict = tool.get_wipe_verdict()
        print(dev, f'{verdict=}')
        for attr, val in vars(secures).items():
            print(f'    {attr}: {val}')
        if opts.wipe and verdict == 'OK':
            print(f'     + start wipe {tool.device_path!r}')
            # --- Monitoring Loop ---
            job = tool.start_wipe(secures.enhanced_erase_supported, db=True)
            while job.process.poll() is None:
                elapsed = round(time.monotonic() - job.wipe_started_mono)
                elapsed_str = f'{elapsed//60}m{elapsed%60}s'
                left = max(job.est_secs - elapsed, 1)
                left_str = f'{left//60}m{left%60}s'
                print(f"Wipe in progress... {elapsed_str} -{left_str}")
                time.sleep(10)
            elapsed = round(time.monotonic() - job.wipe_started_mono)
            elapsed_str = f'{elapsed//60}m{elapsed%60}s'
            print("Wipe complete at {elapsed_str}")
            out, err = job.process.communicate()
            print('\nOUT:\n' + out)
            print('\nERR:\n' + err)

if __name__ == "__main__":
    main()
