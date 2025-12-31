# Quick Start: I/O Throttling

## TL;DR

```bash
sudo dwipe          # Always run as root
# Press 'T' to set throttle speed
# CGroups handle everything
# Done!
```

## Common Scenarios

### Scenario 1: Wipe NVMe Fast, USB Slow (Multi-Device)

```
1. sudo dwipe
2. Select NVMe device
3. Press 'T' until shows [T]=2000MB/s
4. Press 'w' to start wipe

5. Select USB device
6. Press 'T' until shows [T]=50MB/s
7. Press 'w' to start wipe

Result: Both run at their own speeds, no interference ✅
```

### Scenario 2: Maximum Speed (Single Device)

```
1. sudo dwipe
2. Press 'T' to keep at 0 (unlimited)
3. Press 'd' to set dirty_limit_mb=0 (optional)
4. Press 'w' to wipe

Warning: May cause 7-minute drain at end
```

### Scenario 3: Prevent System Freeze

```
1. sudo dwipe
2. Press 'T' until shows reasonable speed (e.g., 500MB/s)
3. Leave dirty_limit_mb at default (2000)
4. Press 'w' to wipe

Result: Controlled rate, no system lockup ✅
```

## Key Takeaways

| Setting | Default | What It Does |
|---------|---------|--------------|
| **throttle_mbps** | 0 | Kernel-enforced rate limit (CGroups) |
| **dirty_limit_mb** | 2000 | Auto-disabled when using CGroups |

## How To Tell It's Working

### Check Current Settings
- Look at header line
- If throttle > 0: Shows `[T]=100MB/s`
- If throttle = 0: No display (unlimited)

### During Wipe
- Watch the rate display
- Should stabilize at your throttle limit
- Multiple devices run independently

## Troubleshooting

**Q: Rate exceeds my throttle limit**
- Temporary burst (normal)
- Should average out over time
- Check dirty pages aren't maxed

**Q: Rate way below throttle limit**
- Device hardware limitation
- USB overhead
- Check device specs

**Q: System still freezes**
- Lower throttle_mbps
- Check dirty_limit_mb is set
- Close other applications

## Pro Tips

1. **Set once, forget it:** Throttle is persistent across restarts
2. **80% rule:** Set throttle to 80% of device max for best results
3. **Watch the first minute:** Rate stabilizes after initial burst
4. **Multi-device:** Set different throttles per device for fairness

## Need More Info?

- Full docs: [THROTTLING.md](THROTTLING.md)
- Implementation: [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)
- Tests: `python3 test_throttle.py`

---

**Remember:** dwipe runs as root → CGroups always available → Just press 'T' and go!
