#!/usr/bin/env python3
"""
dwipe: curses-based tool to wipe physical disks or partitions including
  markers to know their state when wiped.
"""
# pylint: disable=too-many-branches,too-many-statements,import-outside-toplevel
# pylint: disable=too-many-instance-attributes,invalid-name
# pylint: disable=broad-exception-caught,consider-using-with
# pylint: disable=too-many-return-statements,too-many-locals

import os
import sys
import traceback

from .DiskWipe import DiskWipe
from .DeviceInfo import DeviceInfo
from .Utils import Utils
from .Prereqs import Prereqs


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump-lsblk', action='store_true',
                        help='dump parsed lsblk and exit for debugging')
    parser.add_argument('-F', '--firmware-wipes', action='store_true',
                        help='enable experimental (alpha) firmware wipes')
    opts = parser.parse_args()

    dwipe = None  # Initialize to None so exception handler can reference it
    try:
        if os.geteuid() != 0:
            # Re-run the script with sudo needed and opted
            Utils.rerun_module_as_root('dwipe.main')

        prereqs = Prereqs(verbose=True)
        # lsblk is critical for everything; hdparm and nvme only needed for firmware wipes
        if opts.firmware_wipes:
            prereqs.check_all(['lsblk', 'hdparm', 'nvme'])
        else:
            prereqs.check_all(['lsblk'])
        prereqs.report_and_exit_if_failed()

        dwipe = DiskWipe(opts=opts)  # opts=opts)

        dwipe.main_loop()
    except Exception as exce:
        # Try to clean up curses if it was initialized
        try:
            if dwipe and dwipe.win:
                dwipe.win.stop_curses()
        except Exception:
            pass  # Ignore errors during cleanup

        # Always print the error to ensure it's visible
        print("exception:", str(exce))
        print(traceback.format_exc())
        sys.exit(15)


if __name__ == "__main__":
    main()
