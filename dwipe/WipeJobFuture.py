clase WipeJobFuture:
    def __init__(self, device_path, total_size, opts=None, resume_from=0, resume_mode=None):
    """ Performance Throttling """
        # ... existing initialization ...

        # Performance throttling
        self.max_speed_mbps = getattr(opts, 'max_speed_mbps', 0)  # 0 = unlimited
        self.min_speed_mbps = getattr(opts, 'min_speed_mbps', 0)  # Minimum speed to trigger stall detection

        # Stall detection and recovery
        self.stall_timeout = getattr(opts, 'stall_timeout', 300)  # Seconds before stall recovery (5 min)
        self.last_progress_time = time.monotonic()
        self.last_progress_bytes = resume_from

        # Adaptive block sizing
        self.adaptive_block_size = getattr(opts, 'adaptive_block_size', False)
        self.current_write_size = WipeJob.WRITE_SIZE

        # Temperature monitoring (for SSDs)
        self.check_temperature = getattr(opts, 'check_temperature', False)
        self.last_temp_check = 0
        self.temp_check_interval = 60  # seconds

        # Energy-efficient mode
        self.energy_saver = getattr(opts, 'energy_saver', False)




    def _throttle_write_speed(self, bytes_written, start_time):
        """Throttle write speed to specified maximum"""
        if self.max_speed_mbps <= 0:
            return
        
        elapsed = time.monotonic() - start_time
        if elapsed <= 0:
            return
        
        actual_speed_mbps = (bytes_written / (1024 * 1024)) / elapsed
        if actual_speed_mbps > self.max_speed_mbps:
            # Calculate how long to sleep to hit target speed
            target_time = (bytes_written / (1024 * 1024)) / self.max_speed_mbps
            sleep_time = target_time - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)



    def _check_for_stall(self):
        """Check if write progress has stalled"""
        if self.stall_timeout <= 0:
            return False
        
        now = time.monotonic()
        time_since_progress = now - self.last_progress_time
        
        if time_since_progress > self.stall_timeout:
            # Stall detected - try recovery
            self._handle_stall_recovery()
            return True
        
        return False

    def _handle_stall_recovery(self):
        """Attempt to recover from a stalled write"""
        try:
            # 1. Try reducing write size
            if self.adaptive_block_size and self.current_write_size > WipeJob.BLOCK_SIZE:
                self.current_write_size = max(WipeJob.BLOCK_SIZE, self.current_write_size // 2)
                print(f"Stall detected: Reduced write size to {self.current_write_size // 1024}KB")
            
            # 2. Try sync/flush
            os.sync()
            
            # 3. Reset stall timer
            self.last_progress_time = time.monotonic()
            self.last_progress_bytes = self.total_written
            
        except Exception:
            pass






    def _check_ssd_temperature(self):
        """Check SSD temperature and throttle if too hot"""
        if not self.check_temperature:
            return

        now = time.monotonic()
        if now - self.last_temp_check < self.temp_check_interval:
            return

        self.last_temp_check = now

        try:
            # Try to read temperature from SMART or sysfs
            device_name = os.path.basename(self.device_path)

            # Check sysfs for temperature
            temp_paths = [
                f"/sys/block/{device_name}/device/hwmon/hwmon*/temp1_input",
                f"/sys/block/{device_name}/device/temperature",
            ]

            for temp_path in temp_paths:
                for path in glob.glob(temp_path):
                    try:
                        with open(path, 'r') as f:
                            temp_millic = int(f.read().strip())
                            temp_c = temp_millic // 1000

                            if temp_c > 70:  # Throttle if >70°C
                                # Reduce speed by 50%
                                old_max = self.max_speed_mbps
                                self.max_speed_mbps = max(10, old_max // 2)
                                if old_max != self.max_speed_mbps:
                                    print(f"Temperature {temp_c}°C: Throttling to {self.max_speed_mbps} MB/s")
                            elif temp_c < 60 and self.max_speed_mbps != getattr(self.opts, 'max_speed_mbps', 0):
                                # Restore speed if cooled down
                                self.max_speed_mbps = getattr(self.opts, 'max_speed_mbps', 0)

                    except (OSError, ValueError):
                        continue

        except Exception:
            pass






    def _energy_saver_write(self, fd, chunk, is_random_pass):
        """Write with energy saving considerations"""
        if not self.energy_saver:
            return os.write(fd, chunk)

        # For energy saving:
        # 1. Group writes
        # 2. Add small delays between writes
        # 3. Use larger blocks when possible

        bytes_written = 0
        chunk_size = len(chunk)

        while bytes_written < chunk_size:
            write_size = min(self.current_write_size, chunk_size - bytes_written)
            sub_chunk = chunk[bytes_written:bytes_written + write_size]

            # Write the chunk
            written = os.write(fd, sub_chunk)
            bytes_written += written

            # Small delay for energy saving
            if bytes_written < chunk_size:
                time.sleep(0.001)  # 1ms delay

            # Update progress
            self.total_written += written

        return bytes_written







    def _adjust_block_size(self, write_success, write_time):
        """Dynamically adjust block size based on performance"""
        if not self.adaptive_block_size:
            return

        if write_success:
            # Successful write - consider increasing block size
            speed_mbps = (self.current_write_size / (1024 * 1024)) / write_time if write_time > 0 else 0

            if speed_mbps > 100:  # Good speed, try larger blocks
                new_size = min(WipeJob.WRITE_SIZE * 4, self.current_write_size * 2)
                if new_size != self.current_write_size:
                    self.current_write_size = new_size
                    print(f"Increased block size to {new_size // 1024}KB")
        else:
            # Write failed or slow - reduce block size
            new_size = max(WipeJob.BLOCK_SIZE, self.current_write_size // 2)
            if new_size != self.current_write_size:
                self.current_write_size = new_size
                print(f"Reduced block size to {new_size // 1024}KB")















    def get_detailed_status(self):
        """Get detailed status including speed, ETA, and health metrics"""
        elapsed, pct_str, rate_str, eta_str = self.get_status()

        status = {
            'elapsed': elapsed,
            'percentage': pct_str,
            'rate': rate_str,
            'eta': eta_str,
            'bytes_written': self.total_written,
            'total_bytes': self.total_size * self.passes,
            'current_pass': self.current_pass + 1 if not self.verify_phase else 'Verifying',
            'total_passes': self.passes,
            'verify_phase': self.verify_phase,
            'verify_result': self.verify_result if hasattr(self, 'verify_result') else None,
        }

        # Add adaptive block size info if enabled
        if self.adaptive_block_size:
            status['block_size_kb'] = self.current_write_size // 1024

        # Add throttle info if enabled
        if self.max_speed_mbps > 0:
            status['max_speed_mbps'] = self.max_speed_mbps

        return status







    def get_custom_pattern(self, pass_number):
        """Get custom write pattern if specified"""
        custom_patterns = getattr(self.opts, 'custom_patterns', None)
        if custom_patterns and pass_number < len(custom_patterns):
            pattern = custom_patterns[pass_number]
            if pattern == 'random':
                return True
            elif pattern == 'zeros':
                return False
            elif pattern == 'ones':
                # Special pattern: all ones (0xFF)
                return 'ones'
        
        # Fall back to standard pattern
        mode_to_use = self.resume_mode if self.resume_mode else self.opts.wipe_mode.replace('+V', '')
        return self.get_pass_pattern(pass_number, mode_to_use)




    def run_benchmark(self, duration_seconds=30):
        """Run a benchmark to determine optimal settings"""
        print(f"Running benchmark for {duration_seconds} seconds...")

        benchmark_results = []
        test_sizes = [4*1024, 64*1024, 512*1024, 1024*1024, 4*1024*1024]  # 4KB to 4MB

        for test_size in test_sizes:
            self.current_write_size = test_size
            start_time = time.monotonic()
            bytes_written = 0

            # Test write for duration_seconds
            while time.monotonic() - start_time < duration_seconds:
                # Perform test write
                # ... benchmark logic ...
                pass

            speed_mbps = bytes_written / (1024 * 1024) / duration_seconds
            benchmark_results.append((test_size, speed_mbps))
            print(f"  Block size {test_size//1024}KB: {speed_mbps:.2f} MB/s")

        # Find optimal block size
        optimal_size = max(benchmark_results, key=lambda x: x[1])[0]
        print(f"Optimal block size: {optimal_size//1024}KB")

        return optimal_size
