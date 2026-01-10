import os
import mmap
import ctypes
import ctypes.util
from typing import Optional

# Constants from Linux headers
O_DIRECT = 0x4000  # On most systems, but check fcntl.h
PAGE_SIZE = os.sysconf('SC_PAGESIZE')  # Usually 4096

class DirectIOWriter:
    def __init__(self, block_size: int = 4096):
        self.block_size = block_size

        # Get O_DIRECT value from system
        self.o_direct = self._get_o_direct()

    def _get_o_direct(self) -> int:
        """Get O_DIRECT flag value for current system"""
        try:
            # Try to import from fcntl module
            import fcntl
            return fcntl.O_DIRECT
        except AttributeError:
            # Fallback to common values
            return 0x4000  # Most Linux systems

    def allocate_aligned_buffer(self, size: int) -> memoryview:
        """
        Allocate page-aligned memory for O_DIRECT

        Args:
            size: Must be multiple of block_size

        Returns:
            memoryview to the aligned buffer
        """
        if size % self.block_size != 0:
            raise ValueError(f"Size {size} not multiple of block size {self.block_size}")

        # Allocate using mmap for guaranteed page alignment
        # mmap.mmap(-1, size) creates anonymous memory mapping
        aligned_mem = mmap.mmap(-1, size, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)

        # Get memoryview for efficient slicing
        return memoryview(aligned_mem)

    def open_with_odirect(self, path: str, mode: str = "wb"):
        """
        Open file with O_DIRECT flag

        Returns:
            file descriptor (int) not Python file object
        """
        flags = os.O_WRONLY | os.O_CREAT | self.o_direct
        fd = os.open(path, flags, 0o644)
        return fd

    def write_aligned(self, fd: int, data: memoryview, offset: int = None) -> int:
        """
        Write aligned data using O_DIRECT

        Args:
            fd: file descriptor
            data: memoryview of aligned buffer
            offset: file offset (must be block-aligned)

        Returns:
            bytes written
        """
        # Check alignment requirements
        if len(data) % self.block_size != 0:
            raise ValueError(f"Data length {len(data)} not multiple of block size")

        # Check buffer address alignment (optional but good practice)
        if not self._is_buffer_aligned(data):
            raise ValueError("Buffer not properly aligned")

        # Write at current position or seek to offset
        if offset is not None:
            if offset % self.block_size != 0:
                raise ValueError(f"Offset {offset} not aligned to block size")
            os.lseek(fd, offset, os.SEEK_SET)

        # Write with O_DIRECT
        return os.write(fd, data)

    def _is_buffer_aligned(self, mv: memoryview) -> bool:
        """Check if memoryview buffer is properly aligned"""
        # Get underlying buffer address
        buf_ptr = ctypes.c_void_p()
        ctypes.pythonapi.PyObject_AsReadBuffer(
            ctypes.py_object(mv.obj),
            ctypes.byref(buf_ptr),
            ctypes.c_void_p()
        )

        # Check alignment to page size
        return (buf_ptr.value % PAGE_SIZE) == 0

class DiskWiperDirectIO:
    """Complete disk wiper using O_DIRECT"""

    def __init__(self, block_size: int = 4096):
        self.block_size = block_size
        self.writer = DirectIOWriter(block_size)

        # Pre-allocate aligned buffer (e.g., 1MB)
        self.buffer_size = 1024 * 1024  # 1MB
        # Ensure buffer size is multiple of block size
        self.buffer_size = (self.buffer_size + block_size - 1) // block_size * block_size
        self.aligned_buffer = self.writer.allocate_aligned_buffer(self.buffer_size)

        # Fill buffer with zeros
        self.aligned_buffer[:] = b'\x00' * self.buffer_size

    def wipe_device(self, device_path: str, total_bytes: Optional[int] = None):
        """Wipe device using O_DIRECT"""

        # Open device with O_DIRECT
        try:
            flags = os.O_WRONLY | self.writer.o_direct
            fd = os.open(device_path, flags)
        except PermissionError:
            print(f"Need root permissions for {device_path}")
            return

        try:
            # Get device size if not provided
            if total_bytes is None:
                try:
                    total_bytes = os.lseek(fd, 0, os.SEEK_END)
                    os.lseek(fd, 0, os.SEEK_SET)
                except OSError:
                    print("Could not determine device size")
                    return

            # Ensure total size is block-aligned
            total_bytes = (total_bytes // self.block_size) * self.block_size

            # Write in aligned chunks
            bytes_written = 0

            while bytes_written < total_bytes:
                # Calculate chunk size
                remaining = total_bytes - bytes_written
                chunk_size = min(self.buffer_size, remaining)

                # Ensure chunk is block-aligned
                chunk_size = (chunk_size // self.block_size) * self.block_size
                if chunk_size == 0:
                    break

                # Get slice of aligned buffer
                chunk = self.aligned_buffer[:chunk_size]

                # Write with O_DIRECT
                try:
                    written = self.writer.write_aligned(fd, chunk)
                    bytes_written += written

                    # Progress reporting
                    if bytes_written % (100 * 1024 * 1024) < chunk_size:
                        mb = bytes_written / (1024 * 1024)
                        percent = (bytes_written / total_bytes) * 100
                        print(f"Written: {mb:.1f}MB ({percent:.1f}%)")

                except (OSError, ValueError) as e:
                    print(f"Write error at offset {bytes_written}: {e}")
                    # Try falling back to non-O_DIRECT for remainder?
                    break

        finally:
            os.close(fd)
            print(f"Finished: {bytes_written} bytes written")

    def cleanup(self):
        """Clean up allocated buffer"""
        if hasattr(self, 'aligned_buffer'):
            self.aligned_buffer.release()

# Alternative: Using posix_fadvise for cache control
def write_without_cache_hint(fd: int, data: bytes):
    """
    Alternative to O_DIRECT: Use normal writes but hint OS not to cache
    """
    # Write normally
    written = os.write(fd, data)

    # Tell OS we don't need this data in cache
    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
        # posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED)
        libc.posix_fadvise(fd, 0, len(data), 4)  # 4 = POSIX_FADV_DONTNEED
    except:
        pass

    return written

# Simple helper function
def write_odirect_simple(fd: int, data: bytes):
    """
    Simplified O_DIRECT write assuming proper alignment
    """
    # You MUST ensure data is properly aligned!
    # This will crash if data is not aligned

    # Common alignment check
    if (len(data) % 4096 != 0) or (ctypes.addressof(data) % 4096 != 0):
        raise ValueError("Data not properly aligned for O_DIRECT")

    return os.write(fd, data)

# Example usage
def main():
    # Test with a file first
    test_file = "/tmp/test_odirect.bin"

    wiper = DiskWiperDirectIO()

    # Create a test file of specific size
    test_size = 100 * 1024 * 1024  # 100MB
    with open(test_file, "wb") as f:
        f.truncate(test_size)

    try:
        wiper.wipe_device(test_file, test_size)
    finally:
        wiper.cleanup()
        os.unlink(test_file)

if __name__ == "__main__":
    main()
