# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

DuckDB is a high-performance analytical database system designed to be fast, reliable, portable, and easy to use. It is an in-process SQL OLAP database management system with a rich SQL dialect, vectorized execution engine, and columnar storage format.

## Build Commands

### Basic Build
```bash
make debug                   # Builds debug version with sanitizers and assertions
make reldebug                # Builds optimized release version with debug symbols
FORCE_DEBUG=1 make relassert # Builds optimized version with sanitizers and assertions
```

## Testing

### Running Tests
```bash
build/reldebug/test/unittest # Fast unit tests
```

### Running Specific Tests
```bash
# Run specific test file
build/reldebug/test/unittest test/sql/order/test_limit.test

# Run all tests including slow tests
build/reldebug/test/unittest "*"
```

It is recommended to use `make reldebug` and `build/reldebug/test/unittest` unless a good reason exists to use the debug build - the debug build is much slower than the reldebug build.

### Test File Format
Tests use the sqllogictest format (`.test` files). Example structure:
```sql
# name: test/sql/order/test_limit.test
# description: Test LIMIT keyword
# group: [order]

statement ok
CREATE TABLE test (a INTEGER, b INTEGER);

query I
SELECT a FROM test LIMIT 1
----
11

statement error
SELECT a FROM test LIMIT a
----
<REGEX>:Binder Error:.*not found.*
```

Test directives:
- `statement ok` - Statement should succeed
- `statement error` - Statement should fail
- `query I` - Query returning INTEGER column
- `query II` - Query returning two columns
- `----` - Separates query from expected results
- `<REGEX>:` - Expected error message pattern
- `require-env VAR` - Test requires environment variable

Slow tests should use `.test_slow` extension instead of `.test`.

## Code Formatting

```bash
make format-fix        # Format all code (clang-format + black)
```

Ensure you run formatting before committing.

## Architecture

### Query Execution Pipeline

```
SQL String
    ↓
[PARSER] - Uses libpg_query to parse SQL into AST
    ↓
SQLStatement tree (ParsedExpression, TableRef objects)
    ↓
[PLANNER/BINDER] - Binds symbols to catalog, creates logical plan
    ↓
Logical Plan (LogicalOperator tree with bound Expressions)
    ↓
[OPTIMIZER] - Applies rule-based and cost-based optimizations
    ↓
Optimized Logical Plan
    ↓
[PHYSICAL PLAN GENERATOR] - Converts to physical operators
    ↓
Physical Plan (PhysicalOperator tree)
    ↓
[EXECUTOR] - Executes with vectorized, parallel pipelines
    ↓
Results
```

### Core Components

**Parser** (`src/parser/`)
- Converts SQL strings to Abstract Syntax Tree (AST)
- Uses PostgreSQL's libpg_query for parsing
- Outputs: `SQLStatement`, `ParsedExpression`, `TableRef` objects
- Key subdirectories: `expression/`, `statement/`, `tableref/`, `transform/`

**Planner** (`src/planner/`)
- Binds symbols to catalog entries and resolves types
- Creates logical query execution plan
- Key classes: `Binder`, `LogicalOperator`, bound `Expression` types
- Subdirectories: `binder/`, `expression/`, `subquery/`

**Optimizer** (`src/optimizer/`)
- Transforms logical plans without changing semantics
- Applies predicate pushdown, join ordering, expression rewriting, etc.
- Subdirectories: `join_order/`, `statistics/`, `rule/`, `pushdown/`

**Execution Engine** (`src/execution/`)
- Converts logical plan to physical plan and executes
- Push-based vectorized execution model
- Processes data in batches (typically 2048 rows)
- Key subdirectories: `operator/` (scan, join, filter, aggregate, etc.), `expression_executor/`

**Storage** (`src/storage/`)
- Manages persistent data storage and buffer management
- Block-based storage with compression
- Includes WAL (Write-Ahead Log) for durability
- Subdirectories: `buffer/`, `compression/`, `checkpoint/`, `table/`

**Catalog** (`src/catalog/`)
- Metadata management for tables, schemas, functions, types, etc.
- Single source of truth for database metadata
- Key classes: `Catalog`, `CatalogEntry`, `SchemaCatalogEntry`

**Transaction Manager** (`src/transaction/`)
- ACID transaction management with MVCC
- Coordinates concurrent access to data
- Key files: `transaction_manager.cpp`, `undo_buffer.cpp`, `wal_write_state.cpp`

**Parallel Execution** (`src/parallel/`)
- Multi-threaded execution with task scheduling
- Pipeline-based parallelism
- Key files: `executor.cpp`, `pipeline_executor.cpp`, `task_scheduler.cpp`

**Functions** (`src/function/`)
- Built-in function implementations
- Types: `scalar/`, `aggregate/`, `table/`, `window/`, `pragma/`

### Directory Structure

```
/duckdb
├── src/                   # Core C++ source code
│   ├── include/duckdb/   # Public headers
│   ├── parser/           # SQL parsing
│   ├── planner/          # Logical planning
│   ├── optimizer/        # Query optimization
│   ├── execution/        # Physical execution
│   ├── storage/          # Data storage
│   ├── catalog/          # Metadata management
│   ├── transaction/      # Transaction management
│   ├── parallel/         # Parallelization
│   ├── function/         # Built-in functions
│   ├── common/           # Shared utilities and types
│   └── main/             # Database/connection management
├── extension/            # In-tree extensions (parquet, json, icu, etc.)
├── test/                 # Test framework and test cases
│   ├── sql/             # SQL regression tests (.test files)
│   └── api/             # C/C++ API tests
├── tools/                # Language bindings (pythonpkg, shell, etc.)
├── benchmark/            # Benchmark suites (TPC-H, TPC-DS, etc.)
├── scripts/              # Build and utility scripts
└── third_party/          # Third-party dependencies
```

## Extensions

DuckDB supports two types of extensions:

**In-Tree Extensions** (in `extension/` directory):
- Extensions are located in-tree
- Full list in `.github/config/in_tree_extensions.cmake`
- Code can be edited directly and checked into the repository.

**Out-of-Tree Extensions**:
- Extensions are located in a separate git repository
- Full list in `.github/config/out_of_tree_extensions.cmake`
- When changes have to be made, they have to be made in patch files stored in `.github/patches`

Building with extensions:
```bash
# build all extensions
BUILD_ALL_EXT=1 make
# build specific extensions
DUCKDB_EXTENSIONS='json;icu' make
```

## Key Development Patterns

### Data Flow
- **Vectorized Processing**: Data processed in columnar batches (not row-by-row), typically 2048 rows per batch
- **Vector class**: Represents a columnar batch of data
- **ColumnBinding**: Unique identifier `(table_index, column_index)` for columns throughout planning/execution

### Expression Types
- `ParsedExpression` - From parser, unbound
- `Expression` - Bound with type information
- `ExpressionExecutor` - Vectorized execution of expressions

### Memory Management
- Prefer `unique_ptr<T>` for exclusive ownership
- Use `shared_ptr<T>` only when necessary
- `optional_ptr<T>` for nullable references, `reference<T>` for non-nullable references
- Never use raw pointers

### Type System
- `LogicalType` - Abstract data type representation
- Type promotion rules in `src/function/cast_rules.cpp`
- Custom types supported via extension system

### Common Patterns
- **Visitor Pattern**: For tree traversal (e.g., `LogicalOperatorVisitor`, `ExpressionIterator`)
- **Factory Pattern**: `Deserialize()` methods for object creation
- **Class Hierarchy**: Base classes like `*Operator`, `*Entry`, `*Expression` with typed subclasses

## Coding Guidelines (Key Points)

### C++ Style
- Use tabs for indentation, spaces for alignment
- Lines should not exceed 120 columns (run formatter)
- Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, etc.
- Use `idx_t` instead of `size_t` for offsets/indices/counts
- Use `const` references for non-trivial objects
- Use C++11 range-based for loops when possible
- Always use braces for if statements and loops

### Naming Conventions
- **Files**: `snake_case` (e.g., `abstract_operator.cpp`)
- **Types**: `PascalCase` (e.g., `LogicalOperator`)
- **Variables**: `snake_case` (e.g., `chunk_size`)
- **Functions**: `PascalCase` (e.g., `GetChunk`)

### Class Layout
```cpp
class MyClass {
public:
    MyClass();
    int my_public_variable;

public:
    void MyFunction();

private:
    void MyPrivateFunction();

private:
    int my_private_variable;
};
```

### Error Handling
- Use exceptions for query-terminating errors (parser error, table not found, out-of-memory, etc.)
- Use return values for errors that are recoverable during a query
- Use `D_ASSERT` for programmer errors (never triggered by user input)
- Assert liberally with clear comments

### Testing Requirements
- Prefer sqllogictest framework (`.test` files) over C++ tests
- Test with different types (numerics, strings, nested types)
- Test unexpected/incorrect usage, not just happy path
- Slow tests should use `.test_slow` extension
- All tests must pass before submitting PR (`make allunit`)
- Aim for high code coverage

## Navigation Tips

### Finding Components
- Entry point: `src/main/database.cpp` (DatabaseInstance)
- Query execution coordinator: `src/main/client_context.cpp`
- SQL parsing: `src/parser/parser.cpp`
- Logical planning: `src/planner/binder/query_planner.cpp`
- Optimization orchestration: `src/optimizer/optimizer.cpp`
- Physical plan generation: `src/execution/physical_plan/physical_plan_generator.cpp`
- Execution orchestration: `src/parallel/executor.cpp`

### Searching the Codebase
- Use `grep` or `ripgrep` for code search
- Function definitions typically in `.cpp` files
- Class declarations in `src/include/duckdb/` headers
- Test cases in `test/sql/` by functionality

### Understanding a Feature
1. Find test cases in `test/sql/` to see usage examples
2. Trace from parser → planner → optimizer → execution
3. Look for corresponding `*Statement`, `*Operator`, `*Expression` classes
4. Check function registration in catalog

### Modifying Generated Files
Some files are auto-generated. After modifying their sources, run:
```bash
make generate-files
```
This regenerates:
- C API bindings
- Function registration
- Settings
- Serialization code
- Storage info
- Metric enums
- Enum utilities

## Documentation

- Main docs: https://duckdb.org/docs/
- Development docs: https://duckdb.org/dev/
- Build guide: https://duckdb.org/docs/dev/building/overview
- Testing docs: https://duckdb.org/dev/testing

## Important Files

- `Makefile` - Main build configuration
- `CMakeLists.txt` - CMake configuration
- `CONTRIBUTING.md` - Contribution guidelines
- `test/README.md` - Testing documentation
- `extension/extension_config.cmake` - Extension configuration
- `scripts/format.py` - Code formatter
- `scripts/generate_*.py` - Code generation scripts
