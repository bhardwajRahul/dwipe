# dwipe
`dwipe` is a tool to wipe disks and partitions for Linux to help secure your data. `dwipe` aims to reduce mistakes by providing ample information about your devices during selection.

### Quick Comparison

| Feature | dwipe | nwipe | shred | dd |
|---------|-------|-------|-------|-----|
| Interactive TUI | ✓ | ✓ | ✗ | ✗ |
| Multiple simultaneous wipes | ✓ | ✗ | ✗ | ✗ |
| Hot-swap detection | ✓ | ✗ | ✗ | ✗ |
| Device/partition locking | ✓ | ✗ | ✗ | ✗ |
| Persistent wipe state | ✓ | ✗ | ✗ | ✗ |
| Wipe operation logging | ✓ | ✗ | ✗ | ✗ |
| Mount detection/prevention | ✓ | ✓ | ✗ | ✗ |
| Statistical sampling verification | ✓ | ✗ | ✗ | ✗ |
| Multi-pass wipe standards | ✗ | ✓ | ✓ | ✗ |
| Full sequential verification | ✗ | ✓ | ✓ | ✗ |
| Certificate generation | ✗ | ✓ | ✗ | ✗ |

## **V2 Features**

* **Advanced statistical verification** - Automatic or on-demand verification with intelligent pattern detection:
  - Fast-fail for zeros (fails on first non-zero byte)
  - Statistical analysis for random data to check for evidence of randomness
  - Smart sampling: divides disk into 100 sections, randomly samples each section for complete coverage
  - Unmarked disk detection: can verify disks without filesystems and auto-detect if zeros/random
* **Configurable verification percentage** - Choose thoroughness: 0% (skip), 2%, 5%, 10%, 25%, 50%, or 100% (cycle with **V** key, persistent preference)
* **Multi-pass wipe support** - Choose 1, 2, or 4 wipe passes (cycle with **P** key, persistent preference)
* **Inline wipe confirmation** - Confirmation prompts appear below the selected device (no popup), keeping full context visible
* **Configurable confirmation modes** - Choose your safety level: single keypress (Y/y), typed confirmation (YES/yes), or device name (cycle with **c** key)
* **Enhanced wipe history** - Detailed log viewer (**h** key) shows wipe history with UUIDs, filesystems, labels, and percentages for stopped wipes
* **Active wipe highlighting** - In-progress wipes displayed in bright cyan/blue with elapsed time, remaining time, and transfer speed (0-100% write, 101-200% verify)
* **Persistent user preferences** - Theme, wipe mode (random/zeros), confirmation mode, verification %, and locked devices persist across sessions (saved to `~/.config/dwipe/state.json`)
* **Individual partition locking** - Lock individual partitions to prevent accidental wiping (previously only whole disks could be locked)
* **Full terminal color themes** - Complete themed color schemes with backgrounds, not just highlights (cycle with **t** key)
* **Visual feedback improvements** - Mounted and locked devices appear dimmed; active wipes are bright and prominent
* **Smart device identification** - Uses UUID/PARTUUID/serial numbers for stable device tracking across reconnections
* **Screen-based navigation** - Modern screen stack architecture with help screen (**?**) and history screen (**h**)

## Requirements
- **Linux operating system** (uses `/dev/`, `/sys/`, `/proc/` interfaces)
- **Python 3.8 or higher**
- **Root/sudo privileges** (automatically requested when you run the tool)
- **lsblk utility** (usually pre-installed on most Linux distributions)

## Installation

**Recommended (using pipx):**
```bash
pipx install dwipe
```

**Alternative methods:**
```bash
# Using pip
pip install dwipe

# From source
git clone https://github.com/joedefen/dwipe
cd dwipe
pip install .
```

**Verify installation:**
```bash
dwipe --help
```

**Uninstall:**
```bash
pipx uninstall dwipe  # or: pip uninstall dwipe
```

## Quick Start
1. Install `dwipe` using one of the methods above
2. Run `dwipe` from a terminal (sudo will be requested automatically)
3. Observe the context-sensitive help on the first line
4. Navigate with arrow keys or vi-like keys (j/k)
5. Press **?** for full help screen

## Features

`dwipe` provides comprehensive disk wiping capabilities with safety features:

* **Smart device display** - Shows disks and partitions with labels, sizes, types, and vendor/model information to help identify devices correctly
* **Safety protections** - Prevents wiping mounted devices, detects overlapping wipes, supports manual disk locking
* **Hot-swap detection** - Updates the device list when storage changes; newly added devices are marked with **^** to make them easy to spot
* **Multiple simultaneous wipes** - Start wipes on multiple devices at once, with individual progress tracking and completion states
* **Flexible wipe modes** - Choose between filling with random data or zeroing devices (Random mode writes random data then zeros the first 16KB)
* **Persistent state tracking** - Wipe status survives reboots; partially wiped (**s**) and completed (**W**) states are stored on the device
* **Device filtering** - Filter devices by name/pattern using regex in case of too many for one screen
* **Stop capability** - Stop individual wipes or all wipes in progress
* **Disk locking** - Manually lock disks to prevent accidental wipes (locks hide all partitions)
* **Dry-run mode** - Practice using the interface without risk using `--dry-run`

> **Note:** `dwipe` shows file system labels, and if not available, the partition label. It is best practice to label partitions and file systems well to make selection easier.
  
## Usage

Simply run `dwipe` from the command line without arguments:

```bash
dwipe
```

**Command-line options:**
- `--dry-run` or `-n` - Practice mode: test the interface without actually wiping devices
- `--debug` or `-D` - Debug mode (can be repeated for higher verbosity: `-DD`, `-DDD`)

### Color Legend

`dwipe` uses color coding to provide instant visual feedback about device and operation status:

- **Dimmed (gray)** - Mounted or locked devices (cannot be wiped)
- **Default (white)** - Ready to wipe, idle state, or previously wiped (before this session)
- **Bright cyan/blue + bold** - Active wipe or verification in progress (0-100% write, v0-v100% verify)
- **Bold yellow** - Stopped or partially completed wipe
- **Bold green** - ✅ Successfully completed wipe in THIS session (ready to swap out!)
- **Bold orange** - Newly inserted (hot-swapped) device
- **Bold red** - Destructive operation prompts (wipe confirmation)

### Color Themes

`dwipe` supports multiple color themes for improved visibility and aesthetics.

**Available themes:**
- `default` - Terminal Default (basic ANSI colors)
- `dark-mono` - Dark Mono (almost-white on almost-black with bright colors)
- `light-mono` - Light Mono (almost-black on almost-white with bright colors)
- `solarized-dark` - Solarized Dark palette
- `solarized-light` - Solarized Light palette (for light terminal backgrounds)
- `gruvbox` - Gruvbox Dark palette
- `nord` - Nord palette

**Set theme using environment variable:**
```bash
export DWIPE_THEME=solarized-dark
dwipe

# Or inline:
DWIPE_THEME=dark-mono dwipe
DWIPE_THEME=light-mono dwipe
DWIPE_THEME=gruvbox dwipe
DWIPE_THEME=nord dwipe --dry-run
```

**Changing themes:**
- Press **t** from the main screen to open the theme preview screen
- The theme screen shows color examples for each color purpose (DANGER, SUCCESS, WARNING, etc.)
- Press **t** while on the theme screen to cycle through available themes and preview them live
- Press **ESC** or **ENTER** to return to the main screen
- Selected theme is saved and persists across sessions

**Theme features:**
- Yellow/warning color for stopped wipes (state **s**) - highly visible even when not selected
- Red/danger color for wipe confirmation prompts
- Coordinated color palettes designed for terminal readability

Here is a typical screen:

![dwipe-help](https://raw.githubusercontent.com/joedefen/dwipe/master/resources/dwipe-main-screen.png?raw=true)

### Device State Values

The **STATE** column shows the current status of each device:

| State | Meaning |
|-------|---------|
| **-** | Device is ready for wiping |
| **^** | Device is ready for wiping AND was added after `dwipe` started (hot-swapped) |
| **Mnt** | Partition is mounted or disk has mounted partitions - cannot be wiped |
| **N%** | Wipe is in progress (shows percentage complete, 0-100%) |
| **vN%** | Verification is in progress (shows percentage complete, v0-v100%) |
| **STOP** | Wipe or verification is being stopped |
| **s** | Wipe was stopped - device is partially wiped (can restart or verify) |
| **W** | Wipe was completed successfully (can wipe again or verify) |
| **Lock** | Disk is manually locked - partitions are hidden and cannot be wiped |
| **Unlk** | Disk was just unlocked (transitory state) |

### Available Actions

The top line shows available actions. Some are context-sensitive (only available for certain devices):

| Key | Action | Description |
|-----|--------|-------------|
| **w** | wipe | Wipe the selected device (requires confirmation) |
| **v** | verify | Verify a wiped device or detect pattern on unmarked disk (context-sensitive) |
| **s** | stop | Stop the selected wipe in progress (context-sensitive) |
| **S** | Stop All | Stop all wipes in progress |
| **l** | lock/unlock | Lock or unlock a disk to prevent accidental wiping |
| **q** or **x** | quit | Quit the application (stops all wipes first) |
| **?** | help | Show help screen with all actions and navigation keys |
| **h** | history | Show wipe history log |
| **/** | filter | Filter devices by regex pattern (shows matching devices + all active wipes) |
| **ESC** | clear filter | Clear the filter and jump to top of list |
| **m** | mode | Toggle between Rand and Zero wipe modes (saved as preference) |
| **P** | passes | Cycle wipe passes: 1, 2, or 4 (saved as preference) |
| **V** | verify % | Cycle verification percentage: 0%, 2%, 5%, 10%, 25%, 50%, 100% (saved as preference) |
| **c** | confirmation | Cycle confirmation mode: Y, y, YES, yes, device name (saved as preference) |
| **D** | dense | Toggle dense/spaced view (saved as preference) |
| **t** | themes | Open theme preview screen to view and change color themes |

### Wipe Modes

`dwipe` supports two wipe modes (toggle with **m** key):

- **Rand** - Fills the device with random data, then zeros the first 16KB (which contains the wipe metadata)
- **Zero** - Fills the device with zeros (may be faster on some devices due to optimization)

### Verification Strategy

`dwipe` uses intelligent verification with statistical analysis and fast-fail optimizations:

**Smart Sampling:**
- Divides disk into 100 equal sections
- Randomly samples configurable percentage (0%, 2%, 5%, 10%, 25%, 50%, 100%) from EACH section
- Ensures complete disk coverage even with 2% verification
- Change verification percentage with **V** key (saved as preference)

**Pattern Detection:**
- **Zero verification**: Fails immediately on first non-zero byte (fast!)
- **Random verification**: Statistical analysis of byte distribution
  - Tests if byte distribution is uniform (all byte values 0-255 appear fairly equally)
  - Fast-fails periodically if non-random pattern detected
  - Checks for evidence of randomness to distinguish from structured data

**Verification Modes:**
1. **Automatic verification** (after wipe): Set verify % > 0, verification runs after wipe completes
2. **Manual verification** (press **v**): Verify previously wiped devices or detect pattern on unmarked disks
3. **Unmarked disk detection**: Can verify disks with no filesystem to detect if all zeros or random
   - If passes, writes marker as if disk had been wiped
   - Useful for detecting pre-wiped drives or verifying manufacturer erasure

**Verification States:**
- ✓ (green checkmark) - Verification passed
- ✗ (red X) - Verification failed
- No symbol - Not verified
- During verify: **vN%** shows progress (v0% to v100%)

**Why statistical sampling is better than sequential:**
- 2% verification with 100 sections provides better coverage than 2% sequential read
- Detects problems faster (could hit bad sector in early sections)
- Statistical analysis actually validates randomness (sequential can't do this)
- Much faster than 100% sequential verification

### Progress Information

When wiping a device, `dwipe` displays:
- **Write rate** - Current throughput (e.g., "45.2MB/s")
- **Elapsed time** - Time since wipe started
- **Remaining time** - Estimated time to completion

> **Note:** Due to write queueing and caching, initial rates may be inflated, final rates may be deflated, and completion time estimates are optimistic.

### Persistent State

The **W** (wiped) and **s** (partially wiped) states are persistent across reboots. This is achieved by writing metadata to the first 16KB of the device:
- First 15KB: zeros
- Next 1KB: JSON metadata (timestamp, bytes written, total size, mode, verification status)

When a device with persistent state is displayed, additional information shows:
- When it was wiped and the completion percentage
- Verification status: ✓ (passed), ✗ (failed), or no symbol (not verified)


### The Help Screen
When **?** is typed, the help screen looks like:

![dwipe-help](https://raw.githubusercontent.com/joedefen/dwipe/master/resources/dwipe-help-screen.png?raw=true)

### Navigation

You can navigate the device list using:
- **Arrow keys** - Up/Down to move through the list
- **Vi-like keys** - j (down), k (up), g (top), G (bottom)
- **Page Up/Down** - Quick navigation through long lists

## Filter Examples

The **/** filter supports regex patterns. Here are some useful examples:

```
/sda           # Show only sda and its partitions
/sd[ab]        # Show sda, sdb and their partitions
/nvme          # Show all NVMe devices
/nvme0n1p[12]  # Show only partitions 1 and 2 of nvme0n1
/usb           # Show devices with "usb" in their labels
```

Press **ESC** to clear the filter and return to showing all devices.

## Security Considerations

**Important limitations:**

- `dwipe` performs a **single-pass wipe** (not multi-pass like DoD 5220.22-M or Gutmann)
- Adequate for **personal and business data** that doesn't require certified destruction
- **NOT suitable for** classified, top-secret, or highly sensitive data requiring certified multi-pass wiping
- **SSD considerations**:
  - Modern SSDs use wear-leveling and may retain data in unmapped blocks
  - TRIM/DISCARD may prevent complete data erasure
  - For SSDs, consider manufacturer's secure erase utilities for maximum security
  - Random mode may not provide additional security over zeros on SSDs

**Best practices:**
- Verify device labels and sizes carefully before wiping
- Use the **Lock** feature to protect critical disks
- Test with `--dry-run` first if unsure
- Consider encryption for sensitive data as the primary security measure

## Troubleshooting

### dwipe won't start
- **Error: "cannot find lsblk on $PATH"** - Install `util-linux` package
- **Permission denied** - `dwipe` automatically requests sudo; ensure you can use sudo

### Terminal display issues
- **Corrupted display after crash** - Run `reset` or `stty sane` command
- **Colors don't work** - Ensure your terminal supports colors (most modern terminals do)

### Wipe issues
- **Can't wipe a device** - Check the STATE column:
  - **Mnt** - Unmount the partition first: `sudo umount /dev/sdXN`
  - **Lock** - Press **l** to unlock
  - **Busy** - Another partition on the disk is being wiped
- **Wipe is very slow** - Normal for large drives; check write rate to verify progress
- **Wipe seems stuck** - Wait at least 30 seconds for the moving average to stabilize

### Stuck wipe jobs
If a wipe won't stop:
1. Press **s** to stop the selected wipe
2. Wait patiently - stopping can take time as buffers flush
3. If `dwipe` freezes, press Ctrl-Z to suspend, then run `sudo killall -9 python3`

## Development

### Running from source

```bash
git clone https://github.com/joedefen/dwipe
cd dwipe
python3 -m venv venv
source venv/bin/activate
pip install -e .
dwipe --dry-run  # Test without risk
```

### Project structure
- Single-file design: [dwipe/main.py](dwipe/main.py)
- Modern packaging with [Flit](https://flit.pypa.io/)
- Dependency: [console-window](https://pypi.org/project/console-window/) (curses-based TUI framework)

### Contributing
Issues and pull requests welcome at [github.com/joedefen/dwipe](https://github.com/joedefen/dwipe)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [console-window](https://pypi.org/project/console-window/) for the terminal UI
- Uses standard Linux utilities (`lsblk`) for device discovery



---
---
## DETAILED ADVANTAGES / COMPARISONS / USE CASE
### Unique Killer Features
1. Multiple concurrent wipes - The ONLY interactive tool that does this. Wipe 10 drives at once, not one at a time. This alone is game-changing for:
* Data centers
* Computer refurbishment shops
* IT departments doing bulk decommissioning
2. Hot-swap workflow - The color coding makes this brilliant:
* Start 5 drives wiping (cyan)
* Go get coffee
* Come back, instantly see 3 bright green drives = done!
* Pull those 3, pop in 3 new ones
* Repeat
  This workflow is impossible with nwipe or any other tool.
3. Statistical verification - Smarter than full sequential reads:
* 2% verification samples the ENTIRE disk (100 sections)
* Finds problems faster than sequential (could hit bad sector early)
* Fast-fail optimizations: zeros fail on first non-zero byte, random checks for evidence of randomness
* Statistical analysis validates byte distribution uniformity
* Can detect pattern on unmarked disks (auto-detect zeros/random, writes marker if passes)
* Way faster than 100% sequential
4. Safety without sacrificing speed:
* Persistent state (survive crashes/reboots)
* Locking prevents mistakes
* Comprehensive logging with UUIDs
* But still blazing fast concurrent operations
### Compared to Competition:
**vs nwipe:**
* ✅ Dwipe: Multiple simultaneous wipes
* ✅ Dwipe: Hot-swap detection
* ✅ Dwipe: Statistical verification (smarter/faster)
* ✅ Dwipe: Partition-level locking
* ❌ nwipe: DoD standards, certificates (compliance)

**vs shred/dd:**
* ✅ Dwipe: Everything (they're just CLI tools)

** vs enterprise tools:**
* ✅ Dwipe: Free, open source, no licensing
* ✅ Dwipe: Concurrent operations
* ❌ Enterprise: Compliance certifications

**Bottom Line:**
For compliance scenarios (DoD, NIST, certified wipes): Use nwipe. For practical bulk wiping (refurb shops, data centers, IT departments): dwipe is the killer app. Nothing else comes close for the hot-swap concurrent workflow. The green "✅ done!" visual feedback is the cherry on top - it transforms a tedious process into an efficient production line. You've built something genuinely better for real-world use cases. 🚀