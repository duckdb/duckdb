# scripts/metrics/emit_tests.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from .paths import REPO_ROOT, path_from_duckdb, format_file
from .writer import IndentedFileWriter


def _write_statement(f, statement_type, statement):
    f.write(f"statement {statement_type}\n")
    f.write(statement + "\n\n")


def _write_query(f, options, query):
    f.write(f"query {options}\n")
    f.write(query + "\n")
    f.write("----\n")


def _write_default_query(f):
    query = "SELECT unnest(['Maia', 'Thijs', 'Mark', 'Hannes', 'Tom', 'Max', 'Carlo', 'Sam', 'Tania']) AS names ORDER BY random();"
    _write_statement(f, "ok", query)
    _write_statement(f, "ok", "PRAGMA disable_profiling;")


def _write_get_custom_profiling_settings(f):
    query = """
SELECT unnest(res) FROM (
    SELECT current_setting('custom_profiling_settings') AS raw_setting,
    raw_setting.trim('{}') AS setting,
    string_split(setting, ', ') AS res
) ORDER BY ALL;
""".strip()
    _write_query(f, "I", query)


def _write_custom_profiling_optimizer(f):
    _write_statement(f, "ok", "PRAGMA custom_profiling_settings='{\"ALL_OPTIMIZERS\": \"true\"}';")

    _write_default_query(f)

    query = """
SELECT * FROM (
    SELECT unnest(res) str FROM (
        SELECT current_setting('custom_profiling_settings') as raw_setting,
        raw_setting.trim('{}') AS setting,
        string_split(setting, ', ') AS res
    )
) WHERE '"true"' NOT in str
ORDER BY ALL
""".strip()
    _write_query(f, "I", query)
    f.write("\n")

    _write_statement(f, "ok", "PRAGMA custom_profiling_settings='{}'")
    _write_default_query(f)

    _write_get_custom_profiling_settings(f)
    f.write("(empty)\n\n")

    _write_statement(f, "ok", "PRAGMA custom_profiling_settings='{\"OPTIMIZER_JOIN_ORDER\": \"true\"}'")
    _write_default_query(f)

    _write_get_custom_profiling_settings(f)
    f.write("\"OPTIMIZER_JOIN_ORDER\": \"true\"\n\n")

    _write_statement(
        f, "ok", "CREATE OR REPLACE TABLE metrics_output AS SELECT * FROM '__TEST_DIR__/profiling_output.json';"
    )

    query = """
SELECT
    CASE WHEN optimizer_join_order > 0 THEN 'true'
     ELSE 'false' END
FROM metrics_output;
""".strip()
    _write_query(f, "I", query)
    f.write("true\n\n")

    _write_statement(f, "ok", "SET disabled_optimizers = 'JOIN_ORDER';")
    _write_statement(f, "ok", "PRAGMA custom_profiling_settings='{\"OPTIMIZER_JOIN_ORDER\": \"true\"}'")
    _write_default_query(f)

    _write_get_custom_profiling_settings(f)
    f.write("(empty)\n\n")

    _write_statement(f, "ok", "PRAGMA custom_profiling_settings='{\"CUMULATIVE_OPTIMIZER_TIMING\": \"true\"}';")
    _write_default_query(f)

    _write_statement(
        f, "ok", "CREATE OR REPLACE TABLE metrics_output AS SELECT * FROM '__TEST_DIR__/profiling_output.json';"
    )

    query = """
SELECT
    CASE WHEN cumulative_optimizer_timing > 0 THEN 'true'
    ELSE 'false' END
FROM metrics_output;
""".strip()
    _write_query(f, "I", query)
    f.write("true\n\n")

    f.write("# All phase timings must be collected when using detailed profiling mode.\n\n")

    _write_statement(f, "ok", "RESET custom_profiling_settings;")
    _write_statement(f, "ok", "SET profiling_mode = 'detailed';")
    _write_default_query(f)

    query = """
SELECT * FROM (
    SELECT unnest(res) str FROM (
        SELECT current_setting('custom_profiling_settings') AS raw_setting,
        raw_setting.trim('{}') AS setting,
        string_split(setting, ', ') AS res
    )
)
WHERE '"true"' NOT IN str
ORDER BY ALL
""".strip()
    _write_query(f, "I", query)
    f.write("\n")

    _write_statement(f, "ok", "RESET custom_profiling_settings;")
    _write_statement(f, "ok", "SET profiling_mode = 'standard';")


def _generate_group_test(f, groups: list[str], all_metrics: Dict[str, List[str]]):
    _write_statement(f, "ok", "PRAGMA enable_profiling = 'json';")
    _write_statement(f, "ok", "PRAGMA profiling_output = '__TEST_DIR__/profiling_output.json';")

    group_str = ", ".join(f'"{g.upper()}": "true"' for g in groups)
    _write_statement(f, "ok", f"PRAGMA custom_profiling_settings='{{{group_str}}}';")

    _write_default_query(f)
    _write_get_custom_profiling_settings(f)

    metrics: list[str] = []
    for g in groups:
        metrics += all_metrics[g]

    if "all" not in groups and "ALL_OPTIMIZERS" in metrics:
        metrics.extend(all_metrics.get("optimizer", []))

    metrics = list(set(metrics))
    metrics.sort()
    for m in metrics:
        f.write(f'"{m}": "true"\n')
    f.write("\n")

    _write_statement(
        f, "ok", "CREATE OR REPLACE TABLE metrics_output AS SELECT * FROM '__TEST_DIR__/profiling_output.json';"
    )

    cols: list[str] = []
    operator_metrics = set(all_metrics.get("operator", []))
    for m in metrics:
        if m in operator_metrics and "operator" not in groups:
            continue
        cols.append(m)

    select = "SELECT " + ",\n\t".join(cols)

    if "operator" in groups:
        select += "\nFROM (\n"
        select += "\tSELECT unnest(children, max_depth := 2)\n"
        select += "\tFROM metrics_output\n"
        select += ")"
    else:
        select += "\nFROM metrics_output;"

    _write_statement(f, "ok", select)


def _generate_metric_group_test_file(out_path, all_metrics: Dict[str, List[str]]):
    name = path_from_duckdb(out_path)
    print(f"  * {name}")

    top = f"""# name: {name}
# description: Test default profiling settings using groups.
# group: [profiling]

# This file is automatically generated by scripts/generate_metric_enums.py
# Do not edit this file manually, your changes will be overwritten

require json

"""
    with IndentedFileWriter(out_path) as f:
        f.write(top)
        for group in all_metrics:
            _generate_group_test(f, [group], all_metrics)
        _generate_group_test(f, ["default", "file"], all_metrics)
        _generate_group_test(f, ["file", "optimizer"], all_metrics)
        _generate_group_test(f, ["phase_timing", "execution", "file"], all_metrics)
    format_file(out_path)


def _generate_profiling_setting_tests(out_dir: Path, all_metrics: Dict[str, List[str]]):
    test_names = [
        "test_default_profiling_settings",
        "test_custom_profiling_optimizer_settings",
        "test_all_profiling_settings",
    ]
    test_descriptions = ["default", "custom optimizer", "all settings"]
    test_paths = [out_dir / f"{name}.test" for name in test_names]
    metrics_group = ["default", "default", "all"]

    for test_file, name, description, group in zip(test_paths, test_names, test_descriptions, metrics_group):
        display_name = path_from_duckdb(test_file)
        print(f"  * {display_name}")
        with IndentedFileWriter(test_file) as f:
            f.write(f"# name: {display_name}\n")
            f.write(f"# description: Test {description} profiling settings.\n")
            f.write("# group: [profiling]\n\n")
            f.write("# This file is automatically generated by scripts/generate_metric_enums.py\n")
            f.write("# Do not edit this file manually, your changes will be overwritten\n\n")
            f.write("require json\n\n")

            _write_statement(f, "ok", "PRAGMA enable_profiling = 'json';")
            _write_statement(f, "ok", "PRAGMA profiling_output = '__TEST_DIR__/profiling_output.json';")

            mode = "standard" if group == "default" else group
            _write_statement(f, "ok", f"SET profiling_mode='{mode}';")

            if name == "test_custom_profiling_optimizer_settings":
                _write_custom_profiling_optimizer(f)

            _write_default_query(f)
            _write_get_custom_profiling_settings(f)

            for m in all_metrics[group]:
                f.write(f'"{m}": "true"\n')
            f.write("\n")

            _write_statement(
                f, "ok", "CREATE OR REPLACE TABLE metrics_output AS SELECT * FROM '__TEST_DIR__/profiling_output.json';"
            )
            _write_statement(f, "ok", "SELECT cpu_time, extra_info, rows_returned, latency FROM metrics_output;")
        format_file(test_file)


def generate_test_files(out_dir: Path, all_metrics: Dict[str, List[str]]):
    _generate_profiling_setting_tests(out_dir, all_metrics)
    _generate_metric_group_test_file(out_dir / "test_custom_profiling_using_groups.test", all_metrics)
