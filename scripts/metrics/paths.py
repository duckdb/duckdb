# scripts/metrics/paths.py
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
INCLUDE_DIR = SRC_DIR / "include" / "duckdb" / "common" / "enums"
COMMON_ENUMS_DIR = SRC_DIR / "common" / "enums"
TEST_PROFILING_DIR = REPO_ROOT / "test" / "sql" / "pragma" / "profiling"

# Inputs
METRICS_JSON = SRC_DIR / "common" / "enums" / "metric_type.json"
OPTIMIZER_HPP = INCLUDE_DIR / "optimizer_type.hpp"

# Outputs
OUT_METRIC_HPP = INCLUDE_DIR / "metric_type.hpp"
OUT_METRIC_CPP = COMMON_ENUMS_DIR / "metric_type.cpp"
