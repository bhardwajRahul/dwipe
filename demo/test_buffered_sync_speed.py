#!/usr/bin/env python3
"""
Test buffered write + sync speed to a device
Writes buffered for 30s, then syncs, reports net speed, repeats
Usage: sudo ./test_buffered_sync_speed.py /dev/sdb1
"""
import os
import sys
import time

WRITE_SIZE = 1024 * 1024  # 1MB chunks
WRITE_INTERVAL = 30.0  # Write buffered for 30 seconds, then sync

def test_buffered_sync_write(device_path):
    """Write zeros buffered, sync periodically, report net speeds"""

    # Get device size
    try:
        fd = os.open(device_path, os.O_RDONLY)
        device_size = os.lseek(fd, 0, os.SEEK_END)
        os.close(fd)
        print(f"Device: {device_path}")
        print(f"Size: {device_size / (1024**3):.2f} GB")
        print(f"Write size: {WRITE_SIZE / (1024**2):.0f} MB chunks")
        print(f"Write interval: {WRITE_INTERVAL}s (then sync)")
        print()
    except Exception as e:
        print(f"Error getting device size: {e}")
        return

    # Prepare zero buffer
    zero_buffer = bytearray(WRITE_SIZE)

    # Loop forever (until Ctrl+C)
    iteration = 1
    while True:
        print(f"=== Iteration {iteration} ===")

        try:
            # Open buffered (normal mode)
            device = open(device_path, 'wb')

            total_written = 0
            interval_count = 1
            start_time = time.time()

            print(f"Started at {time.strftime('%H:%M:%S')}")
            print()

            while total_written < device_size:
                # Write buffered for WRITE_INTERVAL seconds
                interval_start = time.time()
                interval_written = 0

                print(f"Interval {interval_count}: Writing buffered...", end='', flush=True)

                while time.time() - interval_start < WRITE_INTERVAL:
                    try:
                        bytes_written = device.write(zero_buffer)
                        interval_written += bytes_written
                        total_written += bytes_written

                        if total_written >= device_size:
                            break
                    except Exception as e:
                        print(f"\nWrite error: {e}")
                        break

                write_elapsed = time.time() - interval_start
                write_speed = (interval_written / write_elapsed) / (1024 * 1024)
                print(f" {interval_written / (1024**3):.2f} GB in {write_elapsed:.1f}s "
                      f"({write_speed:.2f} MB/s)", flush=True)

                # Now sync
                print(f"  Syncing...", end='', flush=True)
                sync_start = time.time()
                device.flush()
                os.fsync(device.fileno())
                sync_elapsed = time.time() - sync_start
                print(f" done in {sync_elapsed:.1f}s")

                # Calculate net speed (write + sync)
                total_interval_time = time.time() - interval_start
                net_speed = (interval_written / total_interval_time) / (1024 * 1024)

                total_elapsed = time.time() - start_time
                total_gb = total_written / (1024**3)
                device_gb = device_size / (1024**3)
                percent = (total_written / device_size) * 100

                print(f"  Net speed: {net_speed:.2f} MB/s (including sync)")
                print(f"  Total: {total_gb:.2f}/{device_gb:.2f} GB ({percent:.1f}%) in {int(total_elapsed)}s")
                print()

                interval_count += 1

                if total_written >= device_size:
                    break

            # Close device
            device.close()

            # Final report
            total_elapsed = time.time() - start_time
            avg_speed = (total_written / total_elapsed) / (1024 * 1024)
            print(f"Completed in {int(total_elapsed)}s")
            print(f"Average net speed: {avg_speed:.2f} MB/s")
            print()

        except KeyboardInterrupt:
            print("\n\nStopped by user")
            try:
                device.close()
            except:
                pass
            break
        except Exception as e:
            print(f"\nError: {e}")
            try:
                device.close()
            except:
                pass
            break

        iteration += 1

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: sudo ./test_buffered_sync_speed.py /dev/sdb1")
        sys.exit(1)

    device_path = sys.argv[1]

    if not os.path.exists(device_path):
        print(f"Error: {device_path} does not exist")
        sys.exit(1)

    if os.geteuid() != 0:
        print("Error: Must run as root (use sudo)")
        sys.exit(1)

    print("="*60)
    print("Buffered + Sync Write Speed Test")
    print("="*60)
    print()

    try:
        test_buffered_sync_write(device_path)
    except KeyboardInterrupt:
        print("\n\nStopped by user")
