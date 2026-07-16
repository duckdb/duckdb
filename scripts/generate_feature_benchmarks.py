#!/usr/bin/env python3
"""Generate feature-store benchmarks by sampling a knob space.

Instead of hand-writing one benchmark file per pinned scenario, this script treats every
benchmark as a point in a multi-dimensional *knob space* and samples that space with a
seeded, coverage-guaranteed strategy. This is the combinatorial-interaction-testing
("random list") approach used in modern database-testing research (e.g. SQLancer-style
randomized testing, covering arrays).

Three benchmark families are emitted, each as thin `.benchmark` stubs that select a shared
template and supply the knob values via a `template` block:

  * serve/   -- time SERVE (point-in-time ASOF retrieval).       [pairwise-sampled]
  * refresh/ -- time REFRESH (windowed group-by snapshot).       [pairwise-sampled]
  * cases/   -- curated multi-step scenarios (interleaved        [curated]
                refresh+serve, GC pressure, concurrent refresh).

Sampling strategies for the pairwise families (`--strategy`):
  * pairwise (default): a covering array -- the emitted set provably contains every *pair*
    of knob values (that can co-occur) across any two dimensions at least once. Catches most
    interaction bugs in far fewer runs than the full grid.
  * random: N independent seeded draws (the naive "random list").
  * grid:   the full cross-product (explodes quickly; use only with reduced dimensions).

Reproducibility: everything is driven by `--seed`, so the same seed always emits byte-identical
files and manifest. `--verify` re-checks that the emitted set covers every achievable pair.

Usage:
  python3 scripts/generate_feature_benchmarks.py                 # regenerate all families (seed 42)
  python3 scripts/generate_feature_benchmarks.py --family refresh # just one family (keeps manifest)
  python3 scripts/generate_feature_benchmarks.py --seed 7        # a different reproducible list
  python3 scripts/generate_feature_benchmarks.py --verify        # pairwise coverage self-check
"""
import argparse
import datetime
import itertools
import os
import random

# =============================================================================================
# Knob spaces (dimensions). Each dimension is orthogonal; the generator samples vectors from
# their cross-product subject to per-family constraints.
# =============================================================================================
SERVE_DIMS = {
    "PARQUET_COUNT": [20, 100],  # source scale (number of ClickBench files)
    "AGG": ["simple", "multi", "distinct", "window"],  # feature AS(...) complexity
    "RETAIN": [1, 5, 20],  # retained versions in the store (GC volume)
    "N_REFRESH": [1, 5, 20],  # version depth built before serving
    "TTL": ["0", "30 DAYS"],  # staleness-NULL projection path
    "SPINE_SIZE": [10000, 100000],  # serving spine size (entities served)
    "SERVE_MODE": ["latest", "asof"],  # +infinity probe vs time-travel ASOF
    "SERVE_TS_POS": ["na", "before", "mid", "latest"],  # spine ts vs snapshot timeline
    "N_FEATURES": [1, 3],  # SERVE FEATURE vs SERVE FEATURES a,b,c
    "WRAP": ["bare", "where", "groupby", "cte"],  # SERVE-as-tableref composition
}

# WINDOW is fixed for the SERVE family (window width drives REFRESH cost, not SERVE cost); it is a
# live knob for the REFRESH family below.
WINDOW = "3650 DAYS"

SERVE_CONSTRAINTS_DOC = [
    "SERVE_TS_POS == 'na'  iff  SERVE_MODE == 'latest'  "
    "(latest mode ignores the spine timestamp, so a position is meaningless);",
    "SERVE_TS_POS in {before, mid, latest}  iff  SERVE_MODE == 'asof'.",
]

REFRESH_DIMS = {
    "PARQUET_COUNT": [20, 100],  # source scale (group-by input size)
    "AGG": ["simple", "multi", "distinct", "window"],  # the aggregation being timed
    "WINDOW": [
        "1 DAY",
        "30 DAYS",
        "3650 DAYS",
    ],  # window-filter selectivity (star knob)
    "RETAIN": [1, 5, 20],  # GC volume per refresh
    "PRIOR_VERSIONS": [0, 5],  # store depth already present when the timed refresh runs
}


def is_valid_serve(vec):
    if vec["SERVE_MODE"] == "latest":
        return vec["SERVE_TS_POS"] == "na"
    return vec["SERVE_TS_POS"] in ("before", "mid", "latest")


def sample_serve(rng):
    """Draw a uniformly-random SERVE vector, then repair it to satisfy the constraints."""
    vec = {k: rng.choice(vals) for k, vals in SERVE_DIMS.items()}
    if vec["SERVE_MODE"] == "latest":
        vec["SERVE_TS_POS"] = "na"
    elif vec["SERVE_TS_POS"] == "na":
        vec["SERVE_TS_POS"] = rng.choice(["before", "mid", "latest"])
    return vec


def is_valid_refresh(vec):
    return True  # no cross-dimension constraints


def sample_refresh(rng):
    return {k: rng.choice(vals) for k, vals in REFRESH_DIMS.items()}


# =============================================================================================
# Pairwise covering-array construction (constraint-aware greedy set cover over a seeded pool).
# =============================================================================================
def vector_pairs(vec, dims):
    """Every unordered (dim=value, dim=value) pair present in `vec`, as hashable keys."""
    keys = list(dims.keys())
    out = []
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            a, b = keys[i], keys[j]
            out.append(frozenset(((a, str(vec[a])), (b, str(vec[b])))))
    return out


def achievable_pairs(rng, dims, sampler, pool_size):
    """Pairs that occur in at least one valid vector (the coverage target)."""
    pairs = set()
    for _ in range(pool_size):
        for p in vector_pairs(sampler(rng), dims):
            pairs.add(p)
    return pairs


def build_pairwise(seed, dims, sampler, pool_size=8000):
    """Deterministic greedy covering array. Returns (selected_vectors, achievable, covered)."""
    target = achievable_pairs(random.Random(seed), dims, sampler, pool_size)
    pool_rng = random.Random(seed + 1)
    pool = [sampler(pool_rng) for _ in range(pool_size)]
    covered, selected = set(), []
    while covered != target:
        best, best_gain = None, 0
        for vec in pool:
            gain = sum(1 for p in vector_pairs(vec, dims) if p not in covered)
            if gain > best_gain:
                best, best_gain = vec, gain
        if best is None:
            break  # pool cannot cover the remaining pairs (should not happen: target came from pool)
        selected.append(best)
        covered.update(vector_pairs(best, dims))
    return selected, target, covered


def build_random(seed, sampler, count):
    rng = random.Random(seed)
    return [sampler(rng) for _ in range(count)]


def build_grid(dims, is_valid):
    keys = list(dims.keys())
    out = []
    for combo in itertools.product(*(dims[k] for k in keys)):
        vec = dict(zip(keys, combo))
        if is_valid(vec):
            out.append(vec)
    return out


# =============================================================================================
# SQL fragments shared across families.
# =============================================================================================
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
SIMPLE_AGG = AGG_QUERIES["simple"]

# Secondary features for the multi-feature scenarios. Distinct column names so a WHERE/SELECT on the
# primary feature's `event_count` stays unambiguous.
FEATURE2_AGG = "SELECT UserID, COUNT(*) AS event_count_2, AVG(RegionID) AS avg_region_2 FROM hits GROUP BY UserID"
FEATURE3_AGG = "SELECT UserID, COUNT(*) AS event_count_3 FROM hits GROUP BY UserID"

DROPS = [
    "DROP FEATURE IF EXISTS f",
    "DROP FEATURE IF EXISTS f2",
    "DROP FEATURE IF EXISTS f3",
]

_ANCHOR = datetime.date(2016, 1, 1)  # timestamp of the most recent refresh snapshot
_STEP_DAYS = 30


def _ts(d):
    return d.strftime("%Y-%m-%d 00:00:00")


def join_sql(stmts):
    return "; ".join(stmts) + ";"


def refresh_timestamps(n):
    """n ascending monthly snapshot timestamps ending at the anchor date."""
    return [
        _ts(_ANCHOR - datetime.timedelta(days=_STEP_DAYS * (n - 1 - i)))
        for i in range(n)
    ]


def prior_timestamps(p):
    """p ascending monthly timestamps strictly before the anchor."""
    return [
        _ts(_ANCHOR - datetime.timedelta(days=_STEP_DAYS * (p - i))) for i in range(p)
    ]


def create_feature(name, agg, window, retain, ttl="0"):
    ttl_clause = "" if ttl == "0" else " TTL {}".format(ttl)
    return (
        "CREATE FEATURE {name} ENTITY users TIMESTAMP EventTime WINDOW {window}{ttl} "
        "RETAIN {retain} AS ({agg})"
    ).format(name=name, window=window, ttl=ttl_clause, retain=retain, agg=agg)


def spine_stmt(size=100000):
    """A serving spine whose request timestamp sits just after the anchor snapshot."""
    return (
        "CREATE OR REPLACE TABLE serve_requests AS SELECT DISTINCT UserID, "
        "TIMESTAMP '{ts}' AS EventTime FROM hits USING SAMPLE {n} ROWS".format(
            ts=_ts(_ANCHOR + datetime.timedelta(days=1)), n=size
        )
    )


# =============================================================================================
# SERVE family builders (unchanged behaviour -- keeps the committed serve stubs byte-identical).
# =============================================================================================
def serve_timestamp(vec):
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


def build_serve_setup(vec):
    stmts = list(DROPS)
    stmts.append(
        create_feature("f", AGG_QUERIES[vec["AGG"]], WINDOW, vec["RETAIN"], vec["TTL"])
    )
    if vec["N_FEATURES"] == 3:
        stmts.append(
            create_feature("f2", FEATURE2_AGG, WINDOW, vec["RETAIN"], vec["TTL"])
        )
        stmts.append(
            create_feature("f3", FEATURE3_AGG, WINDOW, vec["RETAIN"], vec["TTL"])
        )
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
    return join_sql(stmts)


def build_serve_run(vec):
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
    return "WITH s AS (SELECT * FROM {}) SELECT count(*) AS n FROM s WHERE event_count IS NOT NULL;".format(
        core
    )


def serve_slug(vec):
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


# =============================================================================================
# REFRESH family builders. Timed statement = a single REFRESH AT the anchor; setup optionally
# pre-builds version depth so the timed refresh runs against a non-empty store.
# =============================================================================================
_WIN_SLUG = {"1 DAY": "1d", "30 DAYS": "30d", "3650 DAYS": "3650d"}


def build_refresh_setup(vec):
    stmts = list(DROPS)
    stmts.append(
        create_feature("f", AGG_QUERIES[vec["AGG"]], vec["WINDOW"], vec["RETAIN"])
    )
    for ts in prior_timestamps(vec["PRIOR_VERSIONS"]):
        stmts.append("REFRESH FEATURE f AT '{}'".format(ts))
    return join_sql(stmts)


def build_refresh_run(vec):
    return "REFRESH FEATURE f AT '{}';".format(_ts(_ANCHOR))


def refresh_slug(vec):
    return "s{pc}_{agg}_w{w}_r{r}_prior{p}".format(
        pc=vec["PARQUET_COUNT"],
        agg=vec["AGG"],
        w=_WIN_SLUG[vec["WINDOW"]],
        r=vec["RETAIN"],
        p=vec["PRIOR_VERSIONS"],
    )


# =============================================================================================
# CASES family: curated multi-step scenarios. RUN_QUERY is a multi-statement sequence on one line,
# timed as a unit. Emitted at each source scale. The cases template sets `require_reinit`, so a
# fresh setup is rebuilt (from the cached source) before every hot run -- keeping the mutating
# refresh timings independent across iterations.
# =============================================================================================
CASE_SCALES = [20, 100]
_CASE_REFRESH_COUNT = 8


def _serve_step():
    return "SELECT count(*) AS n FROM SERVE FEATURE f FOR serve_requests ASOF EventTime"


def case_multi_version_refresh(pc):
    setup = join_sql(DROPS + [create_feature("f", SIMPLE_AGG, "3650 DAYS", 20)])
    run = join_sql(
        [
            "REFRESH FEATURE f AT '{}'".format(ts)
            for ts in refresh_timestamps(_CASE_REFRESH_COUNT)
        ]
    )
    desc = (
        "Build an 8-version refresh history in one timed unit (RETAIN 20, no eviction)"
    )
    return "multi_version_refresh_s{}".format(pc), setup, run, desc


def case_gc_pressure_refresh(pc):
    setup = join_sql(DROPS + [create_feature("f", SIMPLE_AGG, "3650 DAYS", 2)])
    run = join_sql(
        [
            "REFRESH FEATURE f AT '{}'".format(ts)
            for ts in refresh_timestamps(_CASE_REFRESH_COUNT)
        ]
    )
    desc = "8 refreshes with RETAIN 2 -- every refresh past v2 triggers in-place eviction (GC pressure)"
    return "gc_pressure_refresh_s{}".format(pc), setup, run, desc


def case_refresh_then_serve(pc):
    setup = join_sql(
        DROPS + [create_feature("f", SIMPLE_AGG, "3650 DAYS", 5), spine_stmt()]
    )
    run = join_sql(["REFRESH FEATURE f AT '{}'".format(_ts(_ANCHOR)), _serve_step()])
    desc = "Refresh then immediately serve the fresh snapshot (point-in-time ASOF retrieval)"
    return "refresh_then_serve_s{}".format(pc), setup, run, desc


def case_concurrent_feature_refresh(pc):
    anchor = _ts(_ANCHOR)
    setup = join_sql(
        DROPS
        + [
            create_feature("f", SIMPLE_AGG, "3650 DAYS", 5),
            create_feature("f2", FEATURE2_AGG, "3650 DAYS", 5),
            create_feature("f3", FEATURE3_AGG, "3650 DAYS", 5),
        ]
    )
    run = join_sql(
        [
            "REFRESH FEATURE f AT '{}'".format(anchor),
            "REFRESH FEATURE f2 AT '{}'".format(anchor),
            "REFRESH FEATURE f3 AT '{}'".format(anchor),
        ]
    )
    desc = "Refresh three features sharing the same entity key in one timed unit"
    return "concurrent_feature_refresh_s{}".format(pc), setup, run, desc


def case_refresh_serve_interleaved(pc):
    setup = join_sql(
        DROPS + [create_feature("f", SIMPLE_AGG, "3650 DAYS", 5), spine_stmt()]
    )
    anchor = _ts(_ANCHOR)
    steps = []
    for _ in range(3):
        steps.append("REFRESH FEATURE f AT '{}'".format(anchor))
        steps.append(_serve_step())
    run = join_sql(steps)
    desc = "Three interleaved refresh+serve cycles (steady-state multi-version serving)"
    return "refresh_serve_interleaved_s{}".format(pc), setup, run, desc


CASE_BUILDERS = [
    case_multi_version_refresh,
    case_gc_pressure_refresh,
    case_refresh_then_serve,
    case_concurrent_feature_refresh,
    case_refresh_serve_interleaved,
]


def build_cases():
    out = []
    for pc in CASE_SCALES:
        for builder in CASE_BUILDERS:
            name, setup, run, desc = builder(pc)
            out.append(
                {"name": name, "pc": pc, "setup": setup, "run": run, "desc": desc}
            )
    return out


# =============================================================================================
# Family configuration + file emission.
# =============================================================================================
PAIRWISE_FAMILIES = {
    "serve": {
        "subdir": "serve",
        "prefix": "serve_",
        "bench_prefix": "Serve_",
        "desc": "Generated SERVE scenario {name}",
        "dims": SERVE_DIMS,
        "sampler": sample_serve,
        "is_valid": is_valid_serve,
        "constraints": SERVE_CONSTRAINTS_DOC,
        "fixed": ["WINDOW = {}".format(WINDOW)],
        "slug": serve_slug,
        "setup": build_serve_setup,
        "run": build_serve_run,
    },
    "refresh": {
        "subdir": "refresh",
        "prefix": "refresh_",
        "bench_prefix": "Refresh_",
        "desc": "Generated REFRESH scenario {name}",
        "dims": REFRESH_DIMS,
        "sampler": sample_refresh,
        "is_valid": is_valid_refresh,
        "constraints": [],
        "fixed": [
            "TTL disabled",
            "timed statement = REFRESH FEATURE f AT '{}'".format(_ts(_ANCHOR)),
        ],
        "slug": refresh_slug,
        "setup": build_refresh_setup,
        "run": build_refresh_run,
    },
}


def repo_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def stub_text(subdir, prefix, tmpl_rel, bench_name, name, pc, setup, run, seed, desc):
    header_path = "benchmark/feature/clickstream/{}/{}{}.benchmark".format(
        subdir, prefix, name
    )
    return (
        "# name: {path}\n"
        "# description: {desc} (seed={seed})\n"
        "# group: [feature]\n"
        "\n"
        "template {tmpl}\n"
        "BENCH_NAME={bench}\n"
        "PARQUET_COUNT={pc}\n"
        "SETUP_SQL={setup}\n"
        "RUN_QUERY={run}\n"
    ).format(
        path=header_path,
        desc=desc,
        seed=seed,
        tmpl=tmpl_rel,
        bench=bench_name,
        pc=pc,
        setup=setup,
        run=run,
    )


def clean_generated(out_dir, prefix):
    if not os.path.isdir(out_dir):
        return
    for fn in sorted(os.listdir(out_dir)):
        if fn.startswith(prefix) and fn.endswith(".benchmark"):
            os.remove(os.path.join(out_dir, fn))


def emit_pairwise_family(root, fam, cfg, seed, strategy, count):
    if strategy == "pairwise":
        vectors, target, covered = build_pairwise(seed, cfg["dims"], cfg["sampler"])
    elif strategy == "random":
        vectors, target, covered = (
            build_random(seed, cfg["sampler"], count),
            set(),
            set(),
        )
    else:
        vectors, target, covered = (
            build_grid(cfg["dims"], cfg["is_valid"]),
            set(),
            set(),
        )

    subdir = cfg["subdir"]
    out_dir = os.path.join(root, "benchmark/feature/clickstream", subdir)
    tmpl_rel = "benchmark/feature/clickstream/{sd}/{sd}.benchmark.in".format(sd=subdir)
    os.makedirs(out_dir, exist_ok=True)
    clean_generated(out_dir, cfg["prefix"])
    for vec in vectors:
        name = cfg["slug"](vec)
        text = stub_text(
            subdir,
            cfg["prefix"],
            tmpl_rel,
            cfg["bench_prefix"] + name,
            name,
            vec["PARQUET_COUNT"],
            cfg["setup"](vec),
            cfg["run"](vec),
            seed,
            cfg["desc"].format(name=name),
        )
        with open(
            os.path.join(out_dir, "{}{}.benchmark".format(cfg["prefix"], name)), "w"
        ) as f:
            f.write(text)
    return {
        "family": fam,
        "strategy": strategy,
        "vectors": vectors,
        "target": target,
        "covered": covered,
    }


def emit_cases_family(root, seed):
    out_dir = os.path.join(root, "benchmark/feature/clickstream/cases")
    tmpl_rel = "benchmark/feature/clickstream/cases/cases.benchmark.in"
    os.makedirs(out_dir, exist_ok=True)
    clean_generated(out_dir, "case_")
    cases = build_cases()
    for c in cases:
        text = stub_text(
            "cases",
            "case_",
            tmpl_rel,
            "Case_" + c["name"],
            c["name"],
            c["pc"],
            c["setup"],
            c["run"],
            seed,
            c["desc"],
        )
        with open(
            os.path.join(out_dir, "case_{}.benchmark".format(c["name"])), "w"
        ) as f:
            f.write(text)
    return {"family": "cases", "strategy": "curated", "cases": cases}


# =============================================================================================
# Manifest.
# =============================================================================================
def manifest_text(results, seed):
    lines = [
        "# clickstream feature-store benchmark scenario manifest",
        "# Generated by scripts/generate_feature_benchmarks.py -- do not edit by hand.",
        "#",
        "# seed: {}".format(seed),
    ]
    for res in results:
        fam = res["family"]
        lines.append("#")
        if res["strategy"] == "curated":
            cases = res["cases"]
            lines.append(
                "# ===== family: {} (curated, {} scenarios) =====".format(
                    fam, len(cases)
                )
            )
            for c in cases:
                lines.append("#   {:<32} {}".format(c["name"], c["desc"]))
            continue
        vectors = res["vectors"]
        cfg = PAIRWISE_FAMILIES[fam]
        cov = ""
        if res["strategy"] == "pairwise":
            cov = ", {}/{} pairs".format(len(res["covered"]), len(res["target"]))
        lines.append(
            "# ===== family: {} ({}, {} scenarios{}) =====".format(
                fam, res["strategy"], len(vectors), cov
            )
        )
        lines.append("# Dimensions:")
        for k, vals in cfg["dims"].items():
            lines.append("#   {:<14} {}".format(k, vals))
        if cfg["fixed"]:
            lines.append("# Fixed: " + "; ".join(cfg["fixed"]))
        if cfg["constraints"]:
            lines.append("# Constraints:")
            for c in cfg["constraints"]:
                lines.append("#   - {}".format(c))
        header = list(cfg["dims"].keys())
        lines.append("\t".join(["#idx"] + header))
        for i, vec in enumerate(vectors):
            lines.append("\t".join([str(i)] + [str(vec[k]) for k in header]))
    return "\n".join(lines) + "\n"


# =============================================================================================
# Entry point.
# =============================================================================================
def run_verify(seed):
    ok = True
    for fam, cfg in PAIRWISE_FAMILIES.items():
        vectors, _, _ = build_pairwise(seed, cfg["dims"], cfg["sampler"])
        tgt = achievable_pairs(random.Random(seed), cfg["dims"], cfg["sampler"], 8000)
        cov = set()
        for vec in vectors:
            cov.update(vector_pairs(vec, cfg["dims"]))
        missing = tgt - cov
        print(
            "{}: scenarios={} achievable_pairs={} covered={} missing={}".format(
                fam, len(vectors), len(tgt), len(cov & tgt), len(missing)
            )
        )
        if missing:
            ok = False
            for p in sorted(str(sorted(p)) for p in missing):
                print("  UNCOVERED " + p)
    if not ok:
        raise SystemExit(1)
    print("OK: full pairwise coverage for all families.")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--seed", type=int, default=42, help="RNG seed (reproducible list). Default 42."
    )
    ap.add_argument(
        "--family",
        choices=["all", "serve", "refresh", "cases"],
        default="all",
        help="Which family to (re)generate. A single family leaves the shared manifest untouched.",
    )
    ap.add_argument(
        "--strategy", choices=["pairwise", "random", "grid"], default="pairwise"
    )
    ap.add_argument(
        "--count", type=int, default=16, help="Scenario count for --strategy random."
    )
    ap.add_argument(
        "--verify",
        action="store_true",
        help="Only check that each pairwise family covers every achievable pair; write nothing.",
    )
    args = ap.parse_args()

    if args.verify:
        run_verify(args.seed)
        return

    root = repo_root()
    selected = ["serve", "refresh", "cases"] if args.family == "all" else [args.family]
    results = []
    for fam in selected:
        if fam == "cases":
            results.append(emit_cases_family(root, args.seed))
        else:
            results.append(
                emit_pairwise_family(
                    root,
                    fam,
                    PAIRWISE_FAMILIES[fam],
                    args.seed,
                    args.strategy,
                    args.count,
                )
            )

    for res in results:
        n = len(res["cases"]) if res["strategy"] == "curated" else len(res["vectors"])
        extra = ""
        if res["strategy"] == "pairwise":
            extra = " ({}/{} pairs)".format(len(res["covered"]), len(res["target"]))
        print("{:<8} {} scenarios{}".format(res["family"] + ":", n, extra))

    if args.family == "all":
        manifest_path = os.path.join(
            root, "benchmark/feature/clickstream/scenarios.manifest"
        )
        with open(manifest_path, "w") as f:
            f.write(manifest_text(results, args.seed))
        print("Manifest: {}".format(os.path.relpath(manifest_path, root)))
    else:
        print("(single family -- shared scenarios.manifest left unchanged)")


if __name__ == "__main__":
    main()
