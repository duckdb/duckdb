# Advent of Code 2024 `USING KEY` workloads

These workloads are reformulations of recursive SQL solutions in the existing
`benchmark/aoc24` suite. That suite is vendored from Thomas Neumann's
[`aoc24`](https://github.com/neumannt/aoc24) repository under the MIT license
reproduced in `benchmark/aoc24/LICENSE`.

The Day 16 variant keeps only the cheapest cost for each
`(x, y, direction)` state. The Day 20 pair compares its original
history-producing flood with answer-equivalent visited-state tables keyed by
`(x, y)`. The Day 20 load uses a deterministic synthetic race track so that
the ordinary and keyed formulations remain suitable for continuous
regression testing.

Both keyed variants compare candidates with the recurring state through
anti/improvement joins. They therefore exercise bounded `USING KEY` state but
do not use the inner-join-only `RECURSIVE_KEY_JOIN` specialization.
