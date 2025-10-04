# DuckDB CLI Pager Implementation

## Overview

This branch implements a comprehensive CLI pager feature for DuckDB shell, allowing users to view large query results using system pager utilities like `less`, `more`, or specialized pagers like `pspg`.

## Features

### Core Functionality
- **Two modes**: `on` (enabled) and `off` (disabled)
- **Status query**: `.pager` (no arguments) displays current configuration
- **Toggle control**: `.pager on|off` to enable/disable
- **Custom commands**: `.pager 'less -SR'` to set specific pager with options

### Environment Variable Support
- `DUCKDB_PAGER` (first priority)
- `PAGER` (fallback if DUCKDB_PAGER not set)
- Platform defaults: `more` on Windows, none on Unix

### Integration
- Works with all output modes (duckbox, CSV, columnar, JSON, etc.)
- Respects output redirection (no pager when writing to file)
- Automatic cleanup after each query
- Preserves all formatting (headers, nulls, separators)

## Usage Examples

```bash
# Check current pager status
.pager

# Enable pager with environment variable
export DUCKDB_PAGER='less -SR'
.pager on

# Set custom pager command
.pager 'less -SR'           # Less with horizontal scrolling
.pager 'pspg --csv'         # Specialized CSV pager
.pager 'bat --paging=always' # Syntax highlighting pager

# Disable pager
.pager off

# Use with specific output mode
.mode csv
.pager 'pspg --csv'
SELECT * FROM large_table;
```

## Implementation Details

### Modified Files
- `tools/shell/include/shell_state.hpp` - Added `SetupPager()` method declaration
- `tools/shell/shell.cpp` - Added pager implementation (~185 lines)
- `tools/shell/tests/test_pager.py` - Comprehensive test suite (31 tests)

### Technical Approach
1. **Pager Setup**: Called before `ExecutePreparedStatement()`
   - Only activates for stdout console output
   - Uses `popen()` to create pipe to pager process
   - Sets `outfile` to `"|<pager_cmd>"` for tracking

2. **Pager Cleanup**: Called after query execution via `ResetOutput()`
   - Uses `pclose()` with error checking
   - Restores stdout as output
   - Happens for all exit paths (success, error, early return)

3. **Signal Handling**: SIGPIPE ignored to handle user quitting pager early

### Configuration State
- `pager_mode`: `OFF` or `ON`
- `pager_command`: String (can be empty)
- Initialized from `getSystemPager()` at startup

## Test Suite

### Coverage (31 tests, 100% passing)

**Configuration Tests (11)**
- Default status
- Help documentation
- Explicit off
- On without environment variable (warning)
- On with PAGER environment variable
- On with DUCKDB_PAGER environment variable
- DUCKDB_PAGER priority over PAGER
- Custom command
- Custom command with arguments
- Toggle on/off
- Show command integration

**Output Mode Tests (7)**
- Query output
- Large output (100+ rows)
- CSV mode
- Duckbox mode
- Columnar mode
- Mode changes
- Null value preservation

**Integration Tests (7)**
- Output file redirection
- Headers
- Timer output
- Multiple queries
- Multiple statements
- Transactions
- Configuration persistence

**Edge Case Tests (6)**
- Command trimming
- Empty string
- Invalid command error
- Error messages not paged
- Reset to default behavior
- No command configured

### Running Tests

```bash
# Run all pager tests
cd tools/shell/tests
pytest test_pager.py --shell-binary=../../build/release/duckdb -v

# Run specific test
pytest test_pager.py::test_pager_custom_command --shell-binary=../../build/release/duckdb -v
```

## Quality Assurance

✅ Code compiled successfully  
✅ All tests passing (31/31)  
✅ Error handling comprehensive  
✅ Documentation complete  
✅ Follows DuckDB coding standards  
✅ Platform support (Windows + Unix)  
✅ Backwards compatible  
✅ Memory safe (proper cleanup)  
✅ Signal handling (SIGPIPE)  

## Commit History

1. `9c828f4e` - Add paging support for query output
2. `ee4088f6` - Enhance pager environment variable handling
3. `92a2d0c4` - Remove AUTO pager mode and simplify
4. `befbeaec` - Update help documentation
5. `352b131e` - Improve pager setup with error handling
6. `3b857b58` - Add pager status to configuration display
7. `91556668` - Add string trimming utility function
8. `285ed78a` - Refactor pager command handling with validation
9. `612308a8` - Add required includes and SIGPIPE handling
10. `16fbf089` - Refine pager implementation for production quality
11. `4259122` - Add comprehensive test suite for CLI pager functionality

## Performance

- Test suite runs in ~0.20 seconds (31 tests)
- No measurable overhead when pager is off
- Pager process creation is O(1) per query
- Memory usage: minimal (pipe buffer only)

## Known Limitations

- Windows `more` pager has limited features compared to Unix `less`
- Custom pager command persists after being set (not reset to env var on toggle)
- Pager is only used for query output to stdout, not for meta-commands

## Future Enhancements (Optional)

- Auto-detect terminal size and enable pager automatically for large results
- Support for pager-specific options based on detected pager
- Integration with `.once` command for single-query paging
- Pager configuration in `.duckdbrc` initialization file

## Compatibility

- **Minimum DuckDB version**: Current development branch
- **Platform support**: Linux, macOS, Windows
- **Dependencies**: System pager utility (less, more, pspg, etc.)
- **Backwards compatible**: Yes, feature is opt-in

## Documentation

The `.help` command includes complete pager documentation:
```
.pager ?on|off|<cmd>?   Control pager usage for output
   (no args) Display current pager status
   on        Always use pager for output to stdout
   off       Never use pager
   <cmd>     Set custom pager command and enable pager
   Note: Set DUCKDB_PAGER or PAGER environment variable or <cmd> to configure default pager,
         e.g. `.pager 'less -SR'` or `.pager 'pspg --csv'` with `.mode csv`
```

## License

Follows DuckDB's MIT License.
