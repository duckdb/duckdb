# duckdb-cpp example

Minimal external consumer of the duckdb-cpp package: opens an in-memory
database through the stable C++ API, runs a query, reads a value back out.
`CMakeLists.txt` is the full consumer integration; there is nothing else to
wire up.

## Build against released binaries

Once the duckdb-cpp repository and binary channels are public:

    cmake -S . -B build
    cmake --build build
    ./build/example

With nothing set, configure downloads the pinned libduckdb for this platform.
To resolve the library differently, set exactly one of:

    -DDUCKDB_VERSION=1.5.4      # or "nightly": download that instead
    -DDUCKDB_PROVIDER=system    # use an installed library
    -DDUCKDB_ROOT=/path         # use exactly that prefix: an unpacked
                                # libduckdb zip, an install prefix, or a
                                # DuckDB build tree

`-DDUCKDB_DOWNLOAD_BASE_URL=...` points download mode at a mirror or a local
`file://` channel instead of the canonical host.

## Build against this tree (works today)

From the repository root: assemble the package, then point FetchContent's
standard local override at it and use a local engine build.

    python3 scripts/package_cpp_api.py --output /tmp/duckdb-cpp-pkg --package-version 0.1.0-dev
    cmake -S cpp_api/example -B /tmp/example-build \
      -DFETCHCONTENT_SOURCE_DIR_DUCKDB_CPP=/tmp/duckdb-cpp-pkg \
      -DDUCKDB_ROOT=$PWD/build/reldebug
    cmake --build /tmp/example-build
    /tmp/example-build/example

Expected output:

    ┌────────┐
    │ answer │
    │ int32  │
    ├────────┤
    │     42 │
    └────────┘

## Without CMake

The package is build-system neutral: add its directory to the include path,
compile `duckdb_cpp.cpp` into your target, link libduckdb.
