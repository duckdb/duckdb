#!/usr/bin/env python3
"""Generate feature-store SERVE benchmarks by sampling a knob space.

Instead of hand-writing one benchmark file per pinned scenario, this script treats every
benchmark as a point in a multi-dimensional *knob space* and samples that space with a
seeded, coverage-guaranteed strategy. This is the combinatorial-interaction-testing
("random list") approach used in modern database-testing research (e.g. SQLancer-style
randomized testing, covering arrays).

Each sampled knob vector is emitted as a thin `.benchmark` stub that selects the shared
template `benchmark/feature/clickstream/serve/serve.benchmark.in` and supplies the knob
values via a `template` block. The template turns those knobs into the (untimed) feature
setup and the (timed) SERVE query.

Sampling strategies (`--strategy`):
  * pairwise (default): a covering array -- the emitted set provably contains every *pair*
    of knob values (that can co-occur) across any two dimensions at least once. Catches most
    interaction bugs in far fewer runs than the full grid.
  * random: N independent seeded draws (the naive "random list").
  * grid:   the full cross-product (explodes quickly; use only with reduced dimensions).

Reproducibility: everything is driven by `--seed`, so the same seed always emits byte-identical
files and manifest. `--verify` re-checks that the emitted set covers every achievable pair.

Usage:
  python3 scripts/generate_feature_benchmarks.py                 # regenerate the checked-in set
  python3 scripts/generate_feature_benchmarks.py --seed 7        # a different reproducible list
  python3 scripts/generate_feature_benchmarks.py --strategy random --count 12
  python3 scripts/generate_feature_benchmarks.py --verify        # coverage self-check only
"""
import argparse
import datetime
import itertools
import os
import random

# ---------------------------------------------------------------------------------------------
# Knob space (dimensions). Each dimension is orthogonal; the generator samples vectors from
# their cross-product subject to the constraints in `is_valid` below.
# ---------------------------------------------------------------------------------------------
DIMENSIONS = {
    "PARQUET_COUNT": [20, 100],  # source scale (number of ClickBench files)
    "AGG": [
        "simple",
        "multi",
        "distinct",
        "window",
    ],  # feature AS(...) aggregation complexity
    "RETAIN": [1, 5, 20],  # retained versions in the store (GC volume)
    "N_REFRESH": [1, 5, 20],  # version depth built before serving
    "TTL": ["0", "30 DAYS"],  # staleness-NULL projection path
    "SPINE_SIZE": [10000, 100000],  # serving spine size (entities served)
    "SERVE_MODE": ["latest", "asof"],  # +infinity probe vs time-travel ASOF
    "SERVE_TS_POS": [
        "na",
        "before",
        "mid",
        "latest",
    ],  # spine timestamp vs snapshot timeline
    "N_FEATURES": [1, 3],  # SERVE FEATURE vs SERVE FEATURES a,b,c
    "WRAP": ["bare", "where", "groupby", "cte"],  # SERVE-as-tableref composition
}

# WINDOW is fixed for the SERVE family: window width mainly drives REFRESH cost, not SERVE cost,
# and a wide window keeps the stored feature values non-empty. It is a knob for the refresh family.
WINDOW = "3650 DAYS"

CONSTRAINTS_DOC = [
    "SERVE_TS_POS == 'na'  iff  SERVE_MODE == 'latest'  "
    "(latest mode ignores the spine timestamp, so a position is meaningless);",
    "SERVE_TS_POS in {before, mid, latest}  iff  SERVE_MODE == 'asof'.",
]


def is_valid(vec):
    """Reject knob vectors that violate a semantic constraint."""
    if vec["SERVE_MODE"] == "latest":
        return vec["SERVE_TS_POS"] == "na"
    return vec["SERVE_TS_POS"] in ("before", "mid", "latest")


def sample_valid(rng):
    """Draw a uniformly-random vector, then repair it to satisfy the constraints."""
    vec = {k: rng.choice(vals) for k, vals in DIMENSIONS.items()}
    if vec["SERVE_MODE"] == "latest":
        vec["SERVE_TS_POS"] = "na"
    elif vec["SERVE_TS_POS"] == "na":
        vec["SERVE_TS_POS"] = rng.choice(["before", "mid", "latest"])
    return vec


# ---------------------------------------------------------------------------------------------
# Pairwise covering-array construction (constraint-aware greedy set cover over a seeded pool).
# ---------------------------------------------------------------------------------------------
def vector_pairs(vec):
    """Every unordered (dim=value, dim=value) pair present in `vec`, as hashable keys."""
    keys = list(DIMENSIONS.keys())
    out = []
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            a, b = keys[i], keys[j]
            out.append(frozenset(((a, str(vec[a])), (b, str(vec[b])))))
    return out


def achievable_pairs(rng, pool_size):
    """Pairs that occur in at least one valid vector (the coverage target)."""
    pairs = set()
    for _ in range(pool_size):
        for p in vector_pairs(sample_valid(rng)):
            pairs.add(p)
    return pairs


def build_pairwise(seed, pool_size=8000):
    """Deterministic greedy covering array. Returns (selected_vectors, achievable, covered)."""
    target = achievable_pairs(random.Random(seed), pool_size)
    pool_rng = random.Random(seed + 1)
    pool = [sample_valid(pool_rng) for _ in range(pool_size)]
    covered, selected = set(), []
    while covered != target:
        best, best_gain = None, 0
        for vec in pool:
            gain = sum(1 for p in vector_pairs(vec) if p not in covered)
            if gain > best_gain:
                best, best_gain = vec, gain
        if best is None:
            break  # pool cannot cover the remaining pairs (should not happen: target came from pool)
        selected.append(best)
        covered.update(vector_pairs(best))
    return selected, target, covered


def build_random(seed, count):
    rng = random.Random(seed)
    return [sample_valid(rng) for _ in range(count)]


def build_grid():
    keys = list(DIMENSIONS.keys())
    out = []
    for combo in itertools.product(*(DIMENSIONS[k] for k in keys)):
        vec = dict(zip(keys, combo))
        if is_valid(vec):
            out.append(vec)
    return out


# ---------------------------------------------------------------------------------------------
# Turn a knob vector into the SETUP_SQL (untimed) and RUN_QUERY (timed) strings.
# ---------------------------------------------------------------------------------------------
AGG_QUERIES = {
    "simple": "SELECT UserID, COUNT(*) AS event_count FROM hits GROUP BY UserID",
    "multi": (
        "SELECT UserID, COUNT(*) AS event_count, AVG(RegionID) AS avg_region, "
        "MIN(RegionID) AS min_region, MAX(RegionID) AS max_region FROM hits GROUP BY UserID"
    ),
    "distinct": (
        "SELECT UserID, COUNT(DISTINCT RegionID) AS distinct_regions, "
        "COUNT(*) AS event_count FROM hits GROUP BY UserID"
    ),
    # Window-function path. Keeps `hits` as the top-level FROM so the feature's TIMESTAMP column
    # (EventTime) stays resolvable for the refresh window filter; DISTINCT collapses the per-user
    # window values to one row per entity.
    "window": (
        "SELECT DISTINCT UserID, COUNT(*) OVER (PARTITION BY UserID) AS event_count, "
        "AVG(RegionID) OVER (PARTITION BY UserID) AS avg_region FROM hits"
    ),
}

# Secondary features for the multi-feature (SERVE FEATURES) scenarios. Distinct column names so a
# WHERE/SELECT on the primary feature's `event_count` stays unambiguous.
FEATURE2_AGG = "SELECT UserID, COUNT(*) AS event_count_2, AVG(RegionID) AS avg_region_2 FROM hits GROUP BY UserID"
FEATURE3_AGG = "SELECT UserID, COUNT(*) AS event_count_3 FROM hits GROUP BY UserID"

_ANCHOR = datetime.date(2016, 1, 1)  # timestamp of the most recent refresh snapshot
_STEP_DAYS = 30


def _ts(d):
    return d.strftime("%Y-%m-%d 00:00:00")


def refresh_timestamps(n):
    """n ascending monthly snapshot timestamps ending at the anchor date."""
    return [
        _ts(_ANCHOR - datetime.timedelta(days=_STEP_DAYS * (n - 1 - i)))
        for i in range(n)
    ]


def serve_timestamp(vec):
    """Spine request timestamp, positioned relative to the snapshot timeline."""
    rts = refresh_timestamps(vec["N_REFRESH"])
    first = datetime.date.fromisoformat(rts[0][:10])
    last = datetime.date.fromisoformat(rts[-1][:10])
    pos = vec["SERVE_TS_POS"]
    if pos == "before":
        return _ts(first - datetime.timedelta(days=_STEP_DAYS))
    if pos == "latest":
        return _ts(last + datetime.timedelta(days=365))
    if pos == "mid":
        if vec["N_REFRESH"] == 1:
            return _ts(last + datetime.timedelta(days=1))
        mid = datetime.date.fromisoformat(rts[len(rts) // 2][:10])
        return _ts(mid + datetime.timedelta(days=_STEP_DAYS // 2))
    return _ts(_ANCHOR)  # 'na' (latest mode ignores the spine timestamp)


def create_feature(name, agg, vec):
    ttl_clause = "" if vec["TTL"] == "0" else " TTL {}".format(vec["TTL"])
    return (
        "CREATE FEATURE {name} ENTITY users TIMESTAMP EventTime WINDOW {window}{ttl} "
        "RETAIN {retain} AS ({agg})"
    ).format(name=name, window=WINDOW, ttl=ttl_clause, retain=vec["RETAIN"], agg=agg)


def build_setup_sql(vec):
    stmts = [
        "DROP FEATURE IF EXISTS f",
        "DROP FEATURE IF EXISTS f2",
        "DROP FEATURE IF EXISTS f3",
    ]
    stmts.append(create_feature("f", AGG_QUERIES[vec["AGG"]], vec))
    if vec["N_FEATURES"] == 3:
        stmts.append(create_feature("f2", FEATURE2_AGG, vec))
        stmts.append(create_feature("f3", FEATURE3_AGG, vec))
    stmts.append(
        "CREATE OR REPLACE TABLE serve_requests AS SELECT DISTINCT UserID, "
        "TIMESTAMP '{ts}' AS EventTime FROM hits USING SAMPLE {n} ROWS".format(
            ts=serve_timestamp(vec), n=vec["SPINE_SIZE"]
        )
    )
    for ts in refresh_timestamps(vec["N_REFRESH"]):
        stmts.append("REFRESH FEATURE f AT '{}'".format(ts))
        if vec["N_FEATURES"] == 3:
            stmts.append("REFRESH FEATURE f2 AT '{}'".format(ts))
            stmts.append("REFRESH FEATURE f3 AT '{}'".format(ts))
    return "; ".join(stmts) + ";"


def build_run_query(vec):
    if vec["N_FEATURES"] == 1:
        core = "SERVE FEATURE f FOR serve_requests"
    else:
        core = "SERVE FEATURES f, f2, f3 FOR serve_requests"
    if vec["SERVE_MODE"] == "asof":
        core += " ASOF EventTime"
    wrap = vec["WRAP"]
    if wrap == "bare":
        return "SELECT * FROM {};".format(core)
    if wrap == "where":
        return "SELECT * FROM {} WHERE event_count IS NOT NULL;".format(core)
    if wrap == "groupby":
        return (
            "SELECT event_count IS NULL AS missing, count(*) AS n FROM {} "
            "GROUP BY event_count IS NULL;"
        ).format(core)
    # cte
    return "WITH s AS (SELECT * FROM {}) SELECT count(*) AS n FROM s WHERE event_count IS NOT NULL;".format(
        core
    )


# ---------------------------------------------------------------------------------------------
# File emission.
# ---------------------------------------------------------------------------------------------
def slug(vec):
    return "s{pc}_{agg}_r{r}_n{n}_ttl{ttl}_sp{sp}k_{mode}-{pos}_f{nf}_{wrap}".format(
        pc=vec["PARQUET_COUNT"],
        agg=vec["AGG"],
        r=vec["RETAIN"],
        n=vec["N_REFRESH"],
        ttl="30d" if vec["TTL"] != "0" else "0",
        sp=vec["SPINE_SIZE"] // 1000,
        mode=vec["SERVE_MODE"],
        pos=vec["SERVE_TS_POS"],
        nf=vec["N_FEATURES"],
        wrap=vec["WRAP"],
    )


TEMPLATE_REL = "benchmark/feature/clickstream/serve/serve.benchmark.in"
OUT_REL = "benchmark/feature/clickstream/serve"


def stub_text(vec, seed):
    name = slug(vec)
    return (
        "# name: benchmark/feature/clickstream/serve/serve_{name}.benchmark\n"
        "# description: Generated SERVE scenario {name} (seed={seed})\n"
        "# group: [feature]\n"
        "\n"
        "template {tmpl}\n"
        "BENCH_NAME=Serve_{name}\n"
        "PARQUET_COUNT={pc}\n"
        "SETUP_SQL={setup}\n"
        "RUN_QUERY={run}\n"
    ).format(
        name=name,
        seed=seed,
        tmpl=TEMPLATE_REL,
        pc=vec["PARQUET_COUNT"],
        setup=build_setup_sql(vec),
        run=build_run_query(vec),
    )


def manifest_text(vectors, seed, strategy, target, covered):
    lines = []
    lines.append("# clickstream SERVE benchmark scenario manifest")
    lines.append(
        "# Generated by scripts/generate_feature_benchmarks.py -- do not edit by hand."
    )
    lines.append("#")
    lines.append("# seed:        {}".format(seed))
    lines.append("# strategy:    {}".format(strategy))
    lines.append("# scenarios:   {}".format(len(vectors)))
    if strategy == "pairwise":
        lines.append(
            "# pair coverage: {}/{} achievable value-pairs covered".format(
                len(covered), len(target)
            )
        )
    lines.append("# fixed:       WINDOW = {}".format(WINDOW))
    lines.append("#")
    lines.append("# Dimensions:")
    for k, vals in DIMENSIONS.items():
        lines.append("#   {:<14} {}".format(k, vals))
    lines.append("# Constraints:")
    for c in CONSTRAINTS_DOC:
        lines.append("#   - {}".format(c))
    lines.append("")
    header = list(DIMENSIONS.keys())
    lines.append("\t".join(["#idx"] + header))
    for i, vec in enumerate(vectors):
        lines.append("\t".join([str(i)] + [str(vec[k]) for k in header]))
    return "\n".join(lines) + "\n"


def clean_generated(out_dir):
    if not os.path.isdir(out_dir):
        return
    for fn in sorted(os.listdir(out_dir)):
        if fn.startswith("serve_") and fn.endswith(".benchmark"):
            os.remove(os.path.join(out_dir, fn))


def repo_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--seed", type=int, default=42, help="RNG seed (reproducible list). Default 42."
    )
    ap.add_argument(
        "--strategy", choices=["pairwise", "random", "grid"], default="pairwise"
    )
    ap.add_argument(
        "--count",
        type=int,
        default=16,
        help="Number of scenarios for --strategy random.",
    )
    ap.add_argument(
        "--verify",
        action="store_true",
        help="Only check that the emitted set covers every achievable pair; write nothing.",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Override output directory (default: the serve dir).",
    )
    args = ap.parse_args()

    root = repo_root()
    out_dir = args.out_dir or os.path.join(root, OUT_REL)
    manifest_path = os.path.join(
        root, "benchmark/feature/clickstream/scenarios.manifest"
    )

    if args.strategy == "pairwise":
        vectors, target, covered = build_pairwise(args.seed)
    elif args.strategy == "random":
        vectors, target, covered = build_random(args.seed, args.count), set(), set()
    else:
        vectors, target, covered = build_grid(), set(), set()

    if args.verify:
        tgt = achievable_pairs(random.Random(args.seed), 8000)
        cov = set()
        for vec in vectors:
            cov.update(vector_pairs(vec))
        missing = tgt - cov
        print(
            "scenarios={} achievable_pairs={} covered={} missing={}".format(
                len(vectors), len(tgt), len(cov & tgt), len(missing)
            )
        )
        if missing:
            print("UNCOVERED PAIRS:")
            for p in sorted(str(sorted(p)) for p in missing):
                print("  " + p)
            raise SystemExit(1)
        print("OK: full pairwise coverage.")
        return

    os.makedirs(out_dir, exist_ok=True)
    clean_generated(out_dir)
    for vec in vectors:
        path = os.path.join(out_dir, "serve_{}.benchmark".format(slug(vec)))
        with open(path, "w") as f:
            f.write(stub_text(vec, args.seed))
    with open(manifest_path, "w") as f:
        f.write(manifest_text(vectors, args.seed, args.strategy, target, covered))

    print(
        "Wrote {} scenarios to {}".format(len(vectors), os.path.relpath(out_dir, root))
    )
    print("Manifest: {}".format(os.path.relpath(manifest_path, root)))
    if args.strategy == "pairwise":
        print(
            "Pairwise coverage: {}/{} achievable value-pairs.".format(
                len(covered), len(target)
            )
        )


if __name__ == "__main__":
    main()
