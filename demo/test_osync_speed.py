#!/usr/bin/env python3
"""
Test O_SYNC write speed to a device
Usage: sudo ./test_osync_speed.py /dev/sdb1
"""
import os
import sys
import time

WRITE_SIZE = 1024 * 1024  # 1MB chunks
REPORT_INTERVAL = 10.0  # Report every 10 seconds

def test_osync_write(device_path):
    """Write zeros to device with O_SYNC and report speeds"""

    # Get device size
    try:
        fd = os.open(device_path, os.O_RDONLY)
        device_size = os.lseek(fd, 0, os.SEEK_END)
        os.close(fd)
        print(f"Device: {device_path}")
        print(f"Size: {device_size / (1024**3):.2f} GB")
        print(f"Write size: {WRITE_SIZE / (1024**2):.0f} MB chunks")
        print(f"Report interval: {REPORT_INTERVAL}s")
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
            # Open with O_SYNC
            fd = os.open(device_path, os.O_WRONLY | os.O_SYNC)

            total_written = 0
            interval_written = 0
            last_report_time = time.time()
            start_time = time.time()

            print(f"Started at {time.strftime('%H:%M:%S')}")
            print()

            while total_written < device_size:
                # Write chunk
                try:
                    bytes_written = os.write(fd, zero_buffer)
                except Exception as e:
                    print(f"\nWrite error: {e}")
                    break

                total_written += bytes_written
                interval_written += bytes_written

                # Check if time to report
                now = time.time()
                interval_elapsed = now - last_report_time

                if interval_elapsed >= REPORT_INTERVAL:
                    # Calculate speed for last interval
                    speed_mbps = (interval_written / interval_elapsed) / (1024 * 1024)
                    total_elapsed = now - start_time
                    total_gb = total_written / (1024**3)
                    device_gb = device_size / (1024**3)
                    percent = (total_written / device_size) * 100

                    print(f"[{int(total_elapsed):4d}s] {total_gb:6.2f}/{device_gb:.2f} GB ({percent:5.1f}%) | "
                          f"Speed: {speed_mbps:6.2f} MB/s (last {interval_elapsed:.0f}s)")

                    # Reset interval counters
                    interval_written = 0
                    last_report_time = now

            # Close device
            os.close(fd)

            # Final report
            total_elapsed = time.time() - start_time
            avg_speed = (total_written / total_elapsed) / (1024 * 1024)
            print()
            print(f"Completed in {int(total_elapsed)}s")
            print(f"Average speed: {avg_speed:.2f} MB/s")
            print()

        except KeyboardInterrupt:
            print("\n\nStopped by user")
            try:
                os.close(fd)
            except:
                pass
            break
        except Exception as e:
            print(f"\nError: {e}")
            try:
                os.close(fd)
            except:
                pass
            break

        iteration += 1

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: sudo ./test_osync_speed.py /dev/sdb1")
        sys.exit(1)

    device_path = sys.argv[1]

    if not os.path.exists(device_path):
        print(f"Error: {device_path} does not exist")
        sys.exit(1)

    if os.geteuid() != 0:
        print("Error: Must run as root (use sudo)")
        sys.exit(1)

    print("="*60)
    print("O_SYNC Write Speed Test")
    print("="*60)
    print()

    try:
        test_osync_write(device_path)
    except KeyboardInterrupt:
        print("\n\nStopped by user")
