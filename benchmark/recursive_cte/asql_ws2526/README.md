# Advanced SQL recursive workloads

These workloads are adapted from the public teaching examples in
[Advanced SQL WS 2025/26](https://github.com/DBatUTuebingen-Teaching/asql-ws2526)
at commit `2e3a2aecb308b3cef2aac7c2ce1be1dc0cfa654f`.

The original example data is not redistributed. The load scripts generate
deterministic synthetic inputs, and the queries return compact results for
answer validation.

The selected workloads add four recursive execution shapes:

- CYK parsing with two recursive self-references, in ordinary reinjection and
  semi-naive `USING KEY` forms
- one-dimensional diffusion with multiple recursive references and
  per-iteration aggregation
- Marching Squares contour tracing over loop-invariant spatial CTEs
- branching Sudoku search with list-valued recursive state

The CYK and diffusion workloads have ordinary and `USING KEY` variants. The
keyed CYK query combines each new frontier with accumulated parse knowledge
and retains one row per discovered fact. Both diffusion queries compute the
same final state, while the keyed variant retains only the latest value for
each cell.

These adaptations are provided under the MIT license, consistent with the
license the author uses for related teaching and research material.
