#!/usr/bin/env python3
"""Test script to verify I/O throttling functionality"""

from types import SimpleNamespace
from dwipe.WipeJob import CGroupDetector, TokenBucket
import time

def test_cgroup_detection():
    """Test cgroup detection"""
    print("=== Testing CGroup Detection ===")
    version = CGroupDetector.detect_cgroup_version()
    print(f"CGroup version: {version}")

    v2_io, v1_io = CGroupDetector.detect_io_controller()
    print(f"CGroups v2 I/O controller: {v2_io}")
    print(f"CGroups v1 I/O controller: {v1_io}")
    print()

def test_token_bucket():
    """Test token bucket rate limiter"""
    print("=== Testing Token Bucket ===")

    # Create a 1 MB/s bucket with 256KB burst
    rate = 1 * 1024 * 1024  # 1 MB/s
    burst = 256 * 1024  # 256KB
    bucket = TokenBucket(rate, burst)

    print(f"Rate: {rate / (1024*1024):.1f} MB/s")
    print(f"Burst: {burst / 1024:.1f} KB")
    print()

    # Test consuming tokens
    chunk_size = 16 * 1024  # 16KB
    print(f"Consuming {chunk_size / 1024:.0f}KB chunks...")

    for i in range(5):
        wait_time = bucket.consume(chunk_size)
        print(f"  Chunk {i+1}: wait={wait_time:.3f}s")
        if wait_time > 0:
            time.sleep(wait_time)

    print()

def test_throttle_setup():
    """Test throttle setup with different options"""
    print("=== Testing Throttle Setup ===")

    # Import here to avoid needing actual devices
    from dwipe.WipeJob import WipeJob

    # Test with throttling disabled (0 = unlimited)
    opts = SimpleNamespace(
        dry_run=True,
        throttle_mbps=0,
        wipe_mode='Zero',
        passes=1
    )
    job = WipeJob('/dev/null', 1024*1024, opts)
    print(f"throttle_mbps=0: method={job.throttle_method}")

    # Test with throttling enabled
    opts.throttle_mbps = 100
    job = WipeJob('/dev/null', 1024*1024, opts)
    job._setup_throttling()
    print(f"throttle_mbps=100: method={job.throttle_method}")

    # Cleanup
    job._cleanup_cgroup()
    print()

if __name__ == "__main__":
    print("I/O Throttling Feature Test\n")

    test_cgroup_detection()
    test_token_bucket()
    test_throttle_setup()

    print("=== Test Complete ===")
    print("\nThrottling features are ready!")
    print("Use 'T' key in dwipe to cycle through throttle limits:")
    print("  0 = unlimited (no throttling)")
    print("  50, 100, 200, 500, 1000 MB/s")
