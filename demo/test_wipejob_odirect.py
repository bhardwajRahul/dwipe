#!/usr/bin/env python3
"""
Test WipeJob with O_DIRECT implementation
Creates a test file, runs WipeJob on it, verifies the results
"""
import os
import sys
import tempfile
import time
from types import SimpleNamespace

# Add dwipe to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dwipe.WipeJob import WipeJob

def test_wipejob_odirect():
    """Test WipeJob O_DIRECT implementation with a test file"""

    # Create temporary test file (10 MB)
    test_size = 10 * 1024 * 1024

    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        test_file = f.name
        # Create sparse file
        f.truncate(test_size)

    try:
        print(f"Test file: {test_file}")
        print(f"Size: {test_size / (1024**2):.1f} MB")
        print()

        # Create options
        opts = SimpleNamespace(
            wipe_mode='Zero',
            passes=1,
            dry_run=False,
            verify_pct=0
        )

        # Create WipeJob
        print("Creating WipeJob...")
        job = WipeJob(test_file, test_size, opts)

        # Initialize buffers (normally done at module load)
        if WipeJob.buffer is None:
            print("Initializing aligned buffers with mmap...")
            import mmap

            # Allocate random buffer
            WipeJob.buffer_mem = mmap.mmap(-1, WipeJob.BUFFER_SIZE,
                                          flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
            raw_buffer = os.urandom(WipeJob.BUFFER_SIZE)
            rebalanced = WipeJob._rebalance_buffer(raw_buffer)
            WipeJob.buffer_mem.write(rebalanced)
            WipeJob.buffer_mem.seek(0)
            WipeJob.buffer = memoryview(WipeJob.buffer_mem)

            # Allocate zero buffer
            WipeJob.zero_buffer_mem = mmap.mmap(-1, WipeJob.WRITE_SIZE,
                                               flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
            WipeJob.zero_buffer_mem.write(b'\x00' * WipeJob.WRITE_SIZE)
            WipeJob.zero_buffer_mem.seek(0)
            WipeJob.zero_buffer = memoryview(WipeJob.zero_buffer_mem)

            print("Buffers initialized successfully")
            print()

        # Start write
        print("Starting write with O_DIRECT...")
        start_time = time.time()

        job.write_partition()

        elapsed = time.time() - start_time

        # Check results
        print()
        print(f"Write completed in {elapsed:.2f}s")
        print(f"Total written: {job.total_written / (1024**2):.1f} MB")
        print(f"Speed: {(job.total_written / elapsed) / (1024**2):.1f} MB/s")
        print(f"Done: {job.done}")
        print(f"Exception: {job.exception if job.exception else 'None'}")
        print()

        # Verify file is all zeros
        print("Verifying file contents...")
        with open(test_file, 'rb') as f:
            chunk_size = 1024 * 1024
            total_checked = 0
            all_zeros = True

            while total_checked < test_size:
                chunk = f.read(min(chunk_size, test_size - total_checked))
                if chunk != b'\x00' * len(chunk):
                    all_zeros = False
                    print(f"ERROR: Non-zero bytes found at offset {total_checked}")
                    break
                total_checked += len(chunk)

        if all_zeros:
            print("✓ File is all zeros (correct)")
        else:
            print("✗ File contains non-zero bytes (ERROR)")

        print()

        # Check marker
        print("Checking marker...")
        device_name = os.path.basename(test_file)
        marker = WipeJob.read_marker_buffer(device_name)

        if marker:
            print(f"✓ Marker found:")
            print(f"  - scrubbed_bytes: {marker.scrubbed_bytes / (1024**2):.1f} MB")
            print(f"  - size_bytes: {marker.size_bytes / (1024**2):.1f} MB")
            print(f"  - mode: {marker.mode}")
            print(f"  - passes: {marker.passes}")
        else:
            print("✗ No marker found (ERROR)")

        return job.exception is None and all_zeros and marker is not None

    finally:
        # Cleanup
        try:
            os.unlink(test_file)
            print(f"\nCleaned up test file: {test_file}")
        except:
            pass

if __name__ == "__main__":
    print("="*60)
    print("WipeJob O_DIRECT Implementation Test")
    print("="*60)
    print()

    try:
        success = test_wipejob_odirect()
        print()
        if success:
            print("✓ All tests PASSED")
            sys.exit(0)
        else:
            print("✗ Some tests FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
