#!/usr/bin/env python3
"""
Test O_DIRECT write speed using proper alignment technique
Based on o_direct_example.py
Usage: sudo ./test_direct_proper.py /dev/sdb1
"""
import os
import sys
import time
import mmap

BLOCK_SIZE = 4096
WRITE_SIZE = 1024 * 1024  # 1MB chunks (multiple of BLOCK_SIZE)
REPORT_INTERVAL = 10.0

def test_direct_write(device_path):
    """Write zeros to device with O_DIRECT using proper memoryview technique"""

    # Get device size
    try:
        fd = os.open(device_path, os.O_RDONLY)
        device_size = os.lseek(fd, 0, os.SEEK_END)
        os.close(fd)
        print(f"Device: {device_path}")
        print(f"Size: {device_size / (1024**3):.2f} GB")
        print(f"Write size: {WRITE_SIZE / (1024**2):.0f} MB chunks")
        print(f"Block size: {BLOCK_SIZE}")
        print()
    except Exception as e:
        print(f"Error getting device size: {e}")
        return

    # Create properly aligned buffer using mmap
    aligned_mem = mmap.mmap(-1, WRITE_SIZE, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    # Fill with zeros
    aligned_mem[:] = b'\x00' * WRITE_SIZE
    # Get memoryview for efficient writing
    buffer_view = memoryview(aligned_mem)

    iteration = 1
    while True:
        print(f"=== Iteration {iteration} ===")
        print(f"Started at {time.strftime('%H:%M:%S')}")
        print()

        try:
            # Open with O_DIRECT
            flags = os.O_WRONLY | os.O_DIRECT
            fd = os.open(device_path, flags)

            total_written = 0
            interval_written = 0
            last_report_time = time.time()
            start_time = time.time()

            while total_written < device_size:
                # Calculate chunk size
                remaining = device_size - total_written
                chunk_size = min(WRITE_SIZE, remaining)

                # Ensure chunk is block-aligned
                chunk_size = (chunk_size // BLOCK_SIZE) * BLOCK_SIZE
                if chunk_size == 0:
                    break

                # Get slice of buffer (still a memoryview, still aligned!)
                chunk = buffer_view[:chunk_size]

                try:
                    # Write directly from memoryview
                    bytes_written = os.write(fd, chunk)
                    total_written += bytes_written
                    interval_written += bytes_written
                except Exception as e:
                    print(f"\nWrite error at {total_written}: {e}")
                    break

                # Report every interval
                now = time.time()
                interval_elapsed = now - last_report_time

                if interval_elapsed >= REPORT_INTERVAL:
                    speed_mbps = (interval_written / interval_elapsed) / (1024 * 1024)
                    total_elapsed = now - start_time
                    total_gb = total_written / (1024**3)
                    device_gb = device_size / (1024**3)
                    percent = (total_written / device_size) * 100

                    print(f"[{int(total_elapsed):4d}s] {total_gb:6.2f}/{device_gb:.2f} GB "
                          f"({percent:5.1f}%) | Speed: {speed_mbps:6.2f} MB/s (last {interval_elapsed:.0f}s)")

                    interval_written = 0
                    last_report_time = now

            # Close
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
            import traceback
            traceback.print_exc()
            try:
                os.close(fd)
            except:
                pass
            break

        iteration += 1

    # Cleanup
    buffer_view.release()
    aligned_mem.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: sudo ./test_direct_proper.py /dev/sdb1")
        sys.exit(1)

    device_path = sys.argv[1]

    if not os.path.exists(device_path):
        print(f"Error: {device_path} does not exist")
        sys.exit(1)

    if os.geteuid() != 0:
        print("Error: Must run as root")
        sys.exit(1)

    print("="*60)
    print("O_DIRECT Write Speed Test (Proper Alignment)")
    print("="*60)
    print()

    try:
        test_direct_write(device_path)
    except KeyboardInterrupt:
        print("\n\nStopped by user")
