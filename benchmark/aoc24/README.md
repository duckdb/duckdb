# Advent of Code 2024 benchmarks

The SQL files in `queries/` are vendored from Thomas Neumann's
[`aoc24`](https://github.com/neumannt/aoc24) project ("Advent of Code 2024 in SQL").
That upstream project is licensed under the MIT license; a copy of the required
copyright and permission notice is included in this directory as `LICENSE`.

DuckDB-specific integration files in this directory are:

- `*.benchmark` and `aoc24.benchmark.in` for interpreted benchmark execution
- `aoc24.csv` for manually running the AoC benchmark suite through the regression tooling
- `answers/` for the expected benchmark results

The vendored benchmark subset currently covers:
`day01`-`day15`, `day18`-`day21`, `day23`, and `day25`.
