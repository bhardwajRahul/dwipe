#!/usr/bin/env python3
"""
Debug test for WipeJob O_DIRECT
"""
import os
import sys
import mmap

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dwipe.WipeJob import WipeJob
from types import SimpleNamespace

# Create test file
test_file = '/tmp/test_wipejob_debug.bin'
test_size = 10 * 1024 * 1024

# Create file
with open(test_file, 'wb') as f:
    f.truncate(test_size)

print(f"Test file: {test_file}")
print(f"Size: {test_size}")
print()

# Initialize buffers
if WipeJob.buffer is None:
    print("Initializing buffers...")
    WipeJob.buffer_mem = mmap.mmap(-1, WipeJob.BUFFER_SIZE,
                                  flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    raw_buffer = os.urandom(WipeJob.BUFFER_SIZE)
    rebalanced = WipeJob._rebalance_buffer(raw_buffer)
    WipeJob.buffer_mem.write(rebalanced)
    WipeJob.buffer_mem.seek(0)
    WipeJob.buffer = memoryview(WipeJob.buffer_mem)

    WipeJob.zero_buffer_mem = mmap.mmap(-1, WipeJob.WRITE_SIZE,
                                       flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    WipeJob.zero_buffer_mem.write(b'\x00' * WipeJob.WRITE_SIZE)
    WipeJob.zero_buffer_mem.seek(0)
    WipeJob.zero_buffer = memoryview(WipeJob.zero_buffer_mem)
    print("Buffers initialized")
    print()

# Test opening with O_DIRECT
print("Testing O_DIRECT open...")
try:
    fd = os.open(test_file, os.O_WRONLY | os.O_DIRECT)
    print(f"✓ Opened with O_DIRECT: fd={fd}")

    # Try a write
    print("Testing write...")
    chunk = WipeJob.zero_buffer[:4096]
    print(f"Chunk type: {type(chunk)}")
    print(f"Chunk size: {len(chunk)}")

    bytes_written = os.write(fd, chunk)
    print(f"✓ Wrote {bytes_written} bytes")

    os.close(fd)

    # Verify
    with open(test_file, 'rb') as f:
        data = f.read(4096)
        if data == b'\x00' * 4096:
            print("✓ Data verified correctly")
        else:
            print(f"✗ Data mismatch - first 10 bytes: {data[:10]}")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    try:
        os.unlink(test_file)
    except:
        pass
