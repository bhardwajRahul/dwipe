import os
import time
import threading
import subprocess
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Tuple
from collections import deque

class ThrottleMethod(Enum):
    CGroupsV2 = "cgroups_v2"
    CGroupsV1 = "cgroups_v1"
    TOKEN_BUCKET = "token_bucket"
    SLEEP_THROTTLE = "sleep_throttle"

@dataclass
class ThrottleConfig:
    method: ThrottleMethod
    rate_bytes_sec: float
    burst_bytes: float = 10 * 1024 * 1024  # 10MB burst
    use_direct_io: bool = True
    adaptive: bool = True

class TokenBucket:
    """Token bucket rate limiter"""
    def __init__(self, rate_bytes_per_sec, capacity_bytes):
        self.rate = rate_bytes_per_sec
        self.capacity = capacity_bytes
        self.tokens = capacity_bytes
        self.last_update = time.time()
        self.lock = threading.Lock()
        
    def consume(self, tokens):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if tokens > self.tokens:
                deficit = tokens - self.tokens
                wait_time = deficit / self.rate
                return wait_time
            else:
                self.tokens -= tokens
                return 0

class CGroupDetector:
    @staticmethod
    def detect_cgroup_version() -> str:
        """Detect available cgroup version"""
        cgroup_root = "/sys/fs/cgroup"
        
        try:
            with open("/proc/mounts", "r") as f:
                mounts = f.read()
                if "cgroup2" in mounts:
                    return "v2"
                elif "cgroup" in mounts:
                    return "v1"
        except:
            pass
            
        # Check via filesystem
        if os.path.exists(f"{cgroup_root}/cgroup.controllers"):
            return "v2"
        elif os.path.exists(cgroup_root) and os.listdir(cgroup_root):
            return "v1"
            
        return "none"
    
    @staticmethod
    def detect_io_controller() -> Tuple[bool, bool]:
        """Check if I/O controller is available"""
        cgroup_root = "/sys/fs/cgroup"
        has_v2_io = False
        has_v1_io = False
        
        # Check cgroups v2 I/O controller
        try:
            with open(f"{cgroup_root}/cgroup.controllers", "r") as f:
                controllers = f.read()
                has_v2_io = "io" in controllers
        except:
            pass
            
        # Check cgroups v1 blkio controller
        blkio_path = f"{cgroup_root}/blkio"
        has_v1_io = os.path.exists(blkio_path) and os.access(blkio_path, os.W_OK)
        
        return has_v2_io, has_v1_io

class DiskWiperThrottled:
    def __init__(self, target_mbps: float = 100.0):
        self.target_rate = target_mbps * 1024 * 1024
        self.config = self._detect_best_method()
        print(f"Selected throttle method: {self.config.method}")
        
    def _detect_best_method(self) -> ThrottleConfig:
        """Auto-detect best available throttling method"""
        support = self._check_cgroup_support()
        
        if support["version"] == "v2" and support["v2_io"] and support["writable"]:
            return ThrottleConfig(
                method=ThrottleMethod.CGroupsV2,
                rate_bytes_sec=self.target_rate
            )
        elif support["version"] == "v1" and support["v1_io"]:
            return ThrottleConfig(
                method=ThrottleMethod.CGroupsV1,
                rate_bytes_sec=self.target_rate
            )
        else:
            # Fallback to userspace throttling
            return ThrottleConfig(
                method=ThrottleMethod.TOKEN_BUCKET,
                rate_bytes_sec=self.target_rate
            )
    
    def _check_cgroup_support(self) -> dict:
        """Comprehensive cgroup support check"""
        version = CGroupDetector.detect_cgroup_version()
        v2_io, v1_io = CGroupDetector.detect_io_controller()
        
        # Test actual write access
        can_create = False
        if version == "v2" and v2_io:
            test_path = "/sys/fs/cgroup/test.12345"
            try:
                os.makedirs(test_path, exist_ok=False)
                can_create = True
                os.rmdir(test_path)
            except:
                can_create = False
                
        return {
            "version": version,
            "has_io_controller": v2_io or v1_io,
            "v2_io": v2_io,
            "v1_io": v1_io,
            "writable": can_create,
            "kernel_version": os.uname().release
        }
    
    def _setup_cgroup_v2(self, pid: int) -> Optional[str]:
        """Setup cgroup v2 for a process"""
        cgroup_name = f"wiper.{pid}.{threading.get_ident()}"
        cgroup_path = f"/sys/fs/cgroup/{cgroup_name}"
        
        try:
            # Create cgroup
            os.makedirs(cgroup_path, exist_ok=False)
            
            # Enable IO controller
            subtree_file = "/sys/fs/cgroup/cgroup.subtree_control"
            if os.path.exists(subtree_file):
                with open(subtree_file, "r") as f:
                    content = f.read()
                    if "+io" not in content:
                        try:
                            with open(subtree_file, "a") as fw:
                                fw.write("+io\n")
                        except:
                            pass
            
            # Set I/O limit
            io_max = os.path.join(cgroup_path, "io.max")
            with open(io_max, "w") as f:
                f.write(f"0:0 wbps={int(self.target_rate)} rbps=max\n")
            
            # Add process to cgroup
            procs_file = os.path.join(cgroup_path, "cgroup.procs")
            with open(procs_file, "w") as f:
                f.write(str(pid))
            
            return cgroup_path
        except Exception as e:
            print(f"Failed to setup cgroup v2: {e}")
            return None
    
    def _setup_cgroup_v1(self, pid: int) -> Optional[str]:
        """Setup cgroup v1 blkio controller"""
        cgroup_name = f"wiper_{pid}_{threading.get_ident()}"
        blkio_path = f"/sys/fs/cgroup/blkio/{cgroup_name}"
        
        try:
            os.makedirs(blkio_path, exist_ok=False)
            
            # Set write rate limit
            with open(f"{blkio_path}/blkio.throttle.write_bps_device", "w") as f:
                f.write(f"8:0 {int(self.target_rate)}\n")
            
            # Add process
            with open(f"{blkio_path}/tasks", "w") as f:
                f.write(str(pid))
            
            return blkio_path
        except Exception as e:
            print(f"Failed to setup cgroup v1: {e}")
            return None
    
    def _setup_ionice(self):
        """Setup I/O priority as fallback"""
        try:
            subprocess.run(["ionice", "-c", "3", "-p", str(os.getpid())], 
                          capture_output=True)
        except:
            pass
    
    class BufferProvider:
        """Actor 1: Provides buffers to write"""
        def __init__(self, total_bytes: int, chunk_size: int = 1024*1024):
            self.total_bytes = total_bytes
            self.chunk_size = chunk_size
            self.bytes_provided = 0
            self.zero_buffer = b'\x00' * chunk_size
            
        def get_next_buffer(self) -> Optional[bytes]:
            """Returns next buffer or None if done"""
            if self.bytes_provided >= self.total_bytes:
                return None
                
            remaining = self.total_bytes - self.bytes_provided
            chunk = self.zero_buffer[:min(self.chunk_size, remaining)]
            self.bytes_provided += len(chunk)
            return chunk
            
        def get_progress(self) -> float:
            """Returns progress as percentage"""
            return (self.bytes_provided / self.total_bytes) * 100 if self.total_bytes > 0 else 100
    
    class BufferWriter:
        """Actor 2: Writes buffers with throttling"""
        def __init__(self, fd: int, throttle_config: ThrottleConfig):
            self.fd = fd
            self.config = throttle_config
            self.bytes_written = 0
            self.token_bucket = TokenBucket(
                rate_bytes_per_sec=throttle_config.rate_bytes_sec,
                capacity_bytes=throttle_config.burst_bytes
            ) if throttle_config.method == ThrottleMethod.TOKEN_BUCKET else None
            
        def write_buffer(self, buffer: bytes) -> bool:
            """Writes a single buffer with throttling, returns success"""
            if self.token_bucket:
                # Token bucket throttling
                wait_time = self.token_bucket.consume(len(buffer))
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # Write the buffer
            try:
                written = os.write(self.fd, buffer)
                self.bytes_written += written
                
                # Periodic sync (every 100MB)
                if self.bytes_written % (100 * 1024 * 1024) < len(buffer):
                    os.fsync(self.fd)
                    
                return written == len(buffer)
            except Exception as e:
                print(f"Write failed: {e}")
                return False
    
    def _write_loop(self, fd: int, total_bytes: int):
        """Orchestrates the two actors"""
        provider = self.BufferProvider(total_bytes)
        writer = self.BufferWriter(fd, self.config)
        
        while True:
            buffer = provider.get_next_buffer()
            if buffer is None:
                break  # Done
                
            if not writer.write_buffer(buffer):
                break  # Write failed
                
            # Adaptive throttling check (every 5 seconds worth of data)
            if self.config.adaptive:
                bytes_per_5_sec = self.config.rate_bytes_sec * 5
                if writer.bytes_written % bytes_per_5_sec < len(buffer):
                    self._adjust_throttling(provider, writer)
            
            # Progress reporting (every 1%)
            progress = provider.get_progress()
            if int(progress) % 10 == 0 and progress % 1.0 < 0.01:
                print(f"Progress: {progress:.1f}%")
        
        # Final sync
        os.fsync(fd)
        print(f"Complete: {writer.bytes_written} bytes written")
    
    def _adjust_throttling(self, provider, writer):
        """Placeholder for adaptive throttling adjustments"""
        # Could monitor dirty pages, system load, etc.
        pass
    
    def wipe_with_throttle(self, device_path: str):
        """Main wipe function with automatic method selection"""
        pid = os.getpid()
        cgroup_path = None
        
        # Setup throttling based on detected method
        if self.config.method == ThrottleMethod.CGroupsV2:
            cgroup_path = self._setup_cgroup_v2(pid)
            if not cgroup_path:
                print("Falling back to token bucket")
                self.config.method = ThrottleMethod.TOKEN_BUCKET
                
        elif self.config.method == ThrottleMethod.CGroupsV1:
            cgroup_path = self._setup_cgroup_v1(pid)
            if not cgroup_path:
                print("Falling back to token bucket")
                self.config.method = ThrottleMethod.TOKEN_BUCKET
                
        elif self.config.method == ThrottleMethod.TOKEN_BUCKET:
            # Setup ionice as secondary control
            self._setup_ionice()
        
        # Open device with appropriate flags
        flags = os.O_WRONLY
        if self.config.use_direct_io:
            try:
                flags |= os.O_DIRECT
            except AttributeError:
                flags |= os.O_SYNC
        
        try:
            fd = os.open(device_path, flags)
            
            # Get device size
            try:
                total_size = os.lseek(fd, 0, os.SEEK_END)
                os.lseek(fd, 0, os.SEEK_SET)
            except:
                # For files or devices where SEEK_END doesn't work
                total_size = 100 * 1024 * 1024 * 1024  # Default 100GB
            
            # Actual write loop
            self._write_loop(fd, total_size)
            
            os.close(fd)
        except Exception as e:
            print(f"Error wiping {device_path}: {e}")
        finally:
            # Cleanup cgroup if created
            if cgroup_path and os.path.exists(cgroup_path):
                try:
                    # Move processes out before removing
                    procs_file = f"{cgroup_path}/cgroup.procs"
                    if os.path.exists(procs_file):
                        with open(procs_file, "r") as f:
                            pids = f.read().strip().split()
                        
                        # Move to root cgroup
                        root_procs = "/sys/fs/cgroup/cgroup.procs"
                        if os.path.exists(root_procs):
                            with open(root_procs, "a") as f:
                                for pid_str in pids:
                                    f.write(pid_str + "\n")
                    
                    os.rmdir(cgroup_path)
                except:
                    pass  # Best effort cleanup

def main():
    """Example usage"""
    print("=== Disk Wiper with Throttling ===\n")
    
    # Wipe a test file (safer than real device for demo)
    test_file = "/tmp/test_wipe.bin"
    size_mb = 100  # 100 MB test
    
    # Create test file
    print(f"Creating test file: {test_file} ({size_mb} MB)")
    with open(test_file, "wb") as f:
        f.write(b'\x00' * (size_mb * 1024 * 1024))
    
    # Create wiper with 50 MB/s limit
    print("\nStarting wipe with 50 MB/s limit")
    wiper = DiskWiperThrottled(target_mbps=50)
    
    # Run in thread
    def wipe_task():
        wiper.wipe_with_throttle(test_file)
    
    thread = threading.Thread(target=wipe_task, name="Wiper-Thread")
    thread.start()
    
    # Wait for completion
    thread.join()
    
    # Cleanup
    os.unlink(test_file)
    print(f"\nTest complete. File removed.")

if __name__ == "__main__":
    main()
