<div align="center">
  <img src="https://duckdb.org/images/logo-dl/DuckDB_Logo-stacked.svg" height="120">
</div>
<br>

<p align="center">
  <a href="https://github.com/duckdb/duckdb/actions">
    <img src="https://github.com/duckdb/duckdb/actions/workflows/Main.yml/badge.svg?branch=main" alt="Github Actions Badge">
  </a>
  <a href="https://app.codecov.io/gh/duckdb/duckdb">
    <img src="https://codecov.io/gh/duckdb/duckdb/branch/main/graph/badge.svg?token=FaxjcfFghN" alt="codecov"/>
  </a>
  <a href="https://discord.gg/tcvwpjfnZx">
    <img src="https://shields.io/discord/909674491309850675" alt="discord" />
  </a>
  <a href="https://github.com/duckdb/duckdb/releases/">
    <img src="https://img.shields.io/github/v/release/duckdb/duckdb?color=brightgreen&display_name=tag&logo=duckdb&logoColor=white" alt="Latest Release">
  </a>
</p>

## DuckDB Swift
DuckDB Swift is the native Swift API for DuckDB. It employs a modern Swift 
based API for clients across Apple, Linux and Windows platforms.

## DuckDB
DuckDB is a high-performance analytical database system. It is designed to be fast, reliable and easy to use. DuckDB provides a rich SQL dialect, with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs), and more. For more information on the goals of DuckDB, please refer to [the Why DuckDB page on our website](https://duckdb.org/why_duckdb).

## Installation
To use DuckDB in your Swift based project:

1. Add DuckDB to your `Swift.package` dependencies:

    ```swift
    .package(url: "https://github.com/duckdb/duckdb-swift", .upToNextMajor(from: .init(1, 0, 0))),
    ```

2. Add `DuckDB` as a dependency to your target:
 
    ```swift
    .target(name: "TargetName", dependencies: [
      .product(name: "DuckDB", package: "duckdb-swift"),
    ...
    ]),
    ```

## Documentation and Playgrounds
The DuckDB Swift API is fully documented using DocC and the documentation can be generated from within Xcode via `Product > Build Documentation`.

You can also explore an Xcode Playground that demonstrates getting up and running with DuckDB and visualizing data using SwiftUI. To access the playground, clone this repository, open the `DuckDB.xcworkspace` file, and navigate to the `DuckDBPlayground` from within Xcode's project explorer panel.

## Contributing
Issues and Pull Requests should be submitted via [the main DuckDB repository](https://github.com/duckdb/duckdb).

## Development 
Development is managed through [the main DuckDB repository](https://github.com/duckdb/duckdb). To set-up a Swift development environment:

  1. Check out the main DuckDB repository at [`https://github.com/duckdb/duckdb`](https://github.com/duckdb/duckdb)
  2. Pull the latest git tags:
  ```shell
  git fetch --all --tags
  ```
  3. Generate the Unified Build files for the package:
  ```shell
  python3 tools/swift/create_package.py tools/swift
  ```
  4. Open the Xcode workspace at `tools/swift/duckdb-swift/DuckDB.xcworkspace`

Please also refer to our [Contribution Guide](https://github.com/duckdb/duckdb/blob/main/CONTRIBUTING.md).
