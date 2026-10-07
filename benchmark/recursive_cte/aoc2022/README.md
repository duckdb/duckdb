# Advent of Code 2022 recursive workloads

These workloads are adapted from the SQL solutions in
[DBatUTuebingen/Advent_of_Code](https://github.com/DBatUTuebingen/Advent_of_Code)
at commit `10ead5d33412cb6484bf8b9bc4ed91f454c1fd88`.

The upstream repository contains solutions for all 25 days, 17 of which use a
recursive CTE. This directory selects 16 distinct recursive execution shapes:

- Day 5: sequential updates over two nested-list stack states
- Day 7: hierarchical ascent from files to directories
- Day 9: nine chained, narrow rope-following recursions
- Day 11: a long-running state machine with multiple references to the
  recursive CTE
- Day 12: a wide reverse breadth-first search over a height map
- Day 14: a narrow state machine with a growing nested list
- Day 16: directed graph traversal followed by combinatorial path search
- Day 17: bit-packed falling-rock simulation with nested-list state
- Day 18: `UNION`-deduplicated three-dimensional flood fill
- Day 19: branching search with a materialized inner CTE
- Day 20: recursive permutation updates driven by a window
- Day 21: expression-tree evaluation through two recursive self-joins
- Day 22: alternating movement and turn branches in a map state machine
- Day 23: correlated neighborhood aggregation over a moving frontier
- Day 24: moving-hazard precomputation and three dependent traversals
- Day 25: narrow balanced-base-five conversion

The original Advent of Code inputs are not redistributed. The load scripts
generate deterministic synthetic inputs that preserve each recursive plan
shape at a scale suitable for continuous regression testing. The adapted
queries use current DuckDB syntax and return compact checksums or aggregates
for answer validation.

Days 12, 16, 20, and 23 also have answer-equivalent `USING KEY` variants.
Days 12 and 16 use the key to avoid retaining and rescanning redundant
historical states. Days 20 and 23 instead overwrite nearly every keyed row on
every iteration, so they are retained as negative controls: assigning a key
does not help when it does not reduce the recursive work.

The upstream SQL is MIT-licensed. Its license is reproduced in this directory.
