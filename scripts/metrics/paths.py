import os
import subprocess
from pathlib import Path

# Repository root inferred relative to this file
REPO_ROOT = Path(__file__).resolve().parents[2]

# Top-level source directories
SRC_DIR = REPO_ROOT / "src"
INCLUDE_ROOT = SRC_DIR / "include" / "duckdb"

# Subdirectories we target
COMMON_ENUMS_DIR = SRC_DIR / "common" / "enums"
INCLUDE_ENUMS_DIR = INCLUDE_ROOT / "common" / "enums"
INCLUDE_MAIN_DIR = INCLUDE_ROOT / "main"
SRC_MAIN_DIR = SRC_DIR / "main"

# Tests output directory
TEST_PROFILING_DIR = REPO_ROOT / "test" / "sql" / "pragma" / "profiling"

# Inputs
METRICS_JSON = COMMON_ENUMS_DIR / "metric_type.json"
OPTIMIZER_HPP = INCLUDE_ENUMS_DIR / "optimizer_type.hpp"
PROFILING_HPP_TEMPLATE = INCLUDE_MAIN_DIR / "profiling_utils.hpp.template"
PROFILING_CPP_TEMPLATE = SRC_MAIN_DIR / "profiling_utils.cpp.template"

# Outputs
OUT_METRIC_HPP = INCLUDE_ENUMS_DIR / "metric_type.hpp"
OUT_METRIC_CPP = COMMON_ENUMS_DIR / "metric_type.cpp"
OUT_PROFILING_HPP = INCLUDE_MAIN_DIR / "profiling_utils.hpp"
OUT_PROFILING_CPP = SRC_MAIN_DIR / "profiling_utils.cpp"

# Format script
FORMAT_SCRIPT = REPO_ROOT / "scripts" / "format.py"


def path_from_duckdb(path: Path):
    return str(path).split('duckdb/', 1)[1]


def format_file(path: Path):
    print("    └─ Formatting...", end='', flush=True)
    subprocess.run(["python3", FORMAT_SCRIPT, OUT_METRIC_HPP, "--fix", "--noconfirm"], stdout=subprocess.DEVNULL)
    print("\r    └─ Formatted ✓  ")
