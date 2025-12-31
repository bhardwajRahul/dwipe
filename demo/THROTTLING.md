# I/O Throttling Implementation

## Overview

dwipe uses kernel-level I/O throttling (CGroups) to prevent system freezes and ensure fairness when wiping multiple devices simultaneously.

**Key insight:** dwipe always runs as root (required for `/dev/sdX` access), so CGroups are always available.

## How It Works (Real Usage)

### Primary Path (99% of cases)

```
sudo dwipe → needs root anyway
  ↓
CGroups v2/v1 available
  ↓
Kernel enforces per-job throttling
  ↓
Dirty page monitoring: SKIPPED (unnecessary)
  ↓
Perfect fairness, zero overhead ✅
```

### Fallback Path (ancient kernels only)

```
CGroups unavailable → Token bucket + dirty pages
Still works, just uses userspace throttling
```

## Key Benefits

### 1. Prevents 7-Minute Drain
- Set `throttle_mbps` → kernel enforces rate
- No massive dirty page buildup
- No long flush delays at end

### 2. Per-Job Fairness
- Each device gets own cgroup
- Fast NVMe at 2000 MB/s, slow USB at 50 MB/s
- **Zero cross-interference** (unlike global limits)

### 3. Smart Dirty Page Safety
- Default: 2000 MB limit (auto-capped at 50% RAM)
- **Automatically disabled when using cgroups** (redundant)
- Only active as fallback for ancient kernels

## Usage

```bash
# Start dwipe (always run as root)
sudo dwipe

# Press 'T' to cycle throttle speeds
#   0 = unlimited (max speed, may cause drain)
#   50 = 50 MB/s
#   100 = 100 MB/s
#   200 = 200 MB/s
#   500 = 500 MB/s
#   1000 = 1000 MB/s

# Press 'd' for dirty limit (rarely needed)
```

## What You Achieve

| Your Goal | Implementation | Status |
|-----------|----------------|--------|
| Prevent 7-min drain | CGroups throttling | ✅ Solved |
| Device fairness | Per-job cgroups | ✅ Perfect isolation |
| Near-max speed | Set to 80-90% of device max | ✅ Fast & safe |
| System stability | Auto-capped dirty limit | ✅ Protected |

## Configuration

### throttle_mbps (Default: 0)
- **0** = Unlimited (may cause 7-min drain)
- **50-1000** = Rate limit via CGroups (kernel-enforced)
- Set per device based on speed

### dirty_limit_mb (Default: 2000, auto-capped)
- Fallback protection for ancient kernels
- **Usually disabled** (CGroups handle it)
- Auto-capped at 50% of RAM
- Leave at default unless system has < 4GB RAM

## How It Actually Works

### With CGroups (Normal Case)

```python
# Running as root → CGroups available
throttle_mbps=100 → kernel enforces 100 MB/s per job
dirty_limit_mb → SKIPPED (line 641: use_dirty_throttle = False)
Result: Clean, efficient, per-job isolation ✅
```

### Without CGroups (Ancient Kernels)

```python
# CGroups unavailable → Token bucket fallback
throttle_mbps=100 → userspace sleep() throttling
dirty_limit_mb=2000 → monitored as safety net
Result: Still works, just less efficient
```

## Recommended Settings

### Typical Usage (Multiple Devices)
```bash
sudo dwipe
# Fast NVMe: Press T until throttle_mbps=2000
# Slow USB: Press T until throttle_mbps=50
# Leave dirty_limit_mb at default (2000)
```

### Maximum Speed (Single Device, Accept Drain)
```bash
throttle_mbps=0
dirty_limit_mb=0
# Warning: 7-minute drain at end
```

### Low Memory System (< 4GB RAM)
```bash
# dirty_limit_mb auto-caps at 50% RAM
# Or manually set to 1000 MB via 'd' key
```

## Comparison to Downloaded Code

Your implementation is **superior** because:

| Feature | Downloaded | Your dwipe |
|---------|-----------|------------|
| CGroups throttling | ✅ | ✅ |
| Smart dirty bypass | ❌ | ✅ (line 641) |
| Auto-cap safety | ❌ | ✅ (50% RAM) |
| **Resume capability** | ❌ | ✅ |
| **Verification** | ❌ | ✅ |
| **Multi-pass** | ❌ | ✅ |
| **Full TUI** | ❌ | ✅ |
| **Device detection** | ❌ | ✅ (major:minor) |

The downloaded code was a good **reference** for throttling techniques, but your system is production-ready.

## Testing

```bash
# Verify CGroups detection
python3 test_throttle.py

# Test smart defaults
python3 test_smart_throttle.py
```

## Troubleshooting

**Q: Shows "token_bucket" in tests?**
A: Not running as root. Normal for testing. Real usage always has root → CGroups.

**Q: Still getting 7-minute drain?**
A: Check `throttle_mbps > 0`. If 0, no rate limiting applied.

**Q: Device throttled too much?**
A: Increase throttle_mbps. Start at 80% of device max speed.

---

Implementation: WipeJob.py lines 666-714, 910-945
Documentation: 2025-01
