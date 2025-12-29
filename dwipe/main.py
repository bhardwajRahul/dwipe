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


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--dry-run', action='store_true',
                        help='just pretend to zap devices')
    parser.add_argument('-D', '--debug', action='count', default=0,
                        help='debug mode (the more Ds, the higher the debug level)')
    opts = parser.parse_args()

    try:
        if os.geteuid() != 0:
            # Re-run the script with sudo needed and opted
            Utils.rerun_module_as_root('dwipe.main')

        dwipe = DiskWipe()  # opts=opts)
        dwipe.dev_info = info = DeviceInfo(opts=opts)
        dwipe.partitions = info.assemble_partitions()
        if dwipe.DB:
            sys.exit(1)

        dwipe.main_loop()
    except Exception as exce:
        if dwipe and dwipe.win:
            dwipe.win.stop_curses()
        print("exception:", str(exce))
        print(traceback.format_exc())
        sys.exit(15)


if __name__ == "__main__":
    main()
