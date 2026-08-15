import base64
import gzip
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import textwrap
import unittest
from unittest import mock

from scripts import samply_profile


def synthetic_profile():
    return {
        "meta": {
            "startTime": 1000.0,
            "categories": [{"name": "Other", "color": "grey"}],
            "markerSchema": [],
        },
        "threads": [
            {"pid": 7, "processStartupTime": 0.0, "processShutdownTime": None},
            {"pid": 42, "processStartupTime": 50.0, "processShutdownTime": 300.0},
            {"pid": 42, "processStartupTime": 0.0, "processShutdownTime": None},
        ],
        "counters": [{"name": "existing"}],
    }


def _http_record(
    start_unix_ns,
    duration_ns,
    status_code,
    bytes_received,
    time_to_first_byte_ns,
    method,
    url,
    request_range="",
    response_content_range="",
):
    encoded = [
        base64.b64encode(value.encode("utf-8")).decode("ascii")
        for value in (url, request_range, response_content_range)
    ]
    return "\t".join(
        map(
            str,
            [
                1,
                start_unix_ns,
                duration_ns,
                status_code,
                bytes_received,
                time_to_first_byte_ns,
                method,
                *encoded,
            ],
        )
    )


def _add_marker_table(thread, *, main=False):
    thread["isMainThread"] = main
    thread["stringArray"] = ["Existing marker"]
    thread["markers"] = {
        "category": [0],
        "data": [None],
        "endTime": [60.0],
        "length": 1,
        "name": [0],
        "phase": [0],
        "startTime": [60.0],
    }


class SamplyInjectionTests(unittest.TestCase):
    def test_injects_all_counters_with_pid_and_lifetime_filtering(self):
        with tempfile.TemporaryDirectory() as directory_name:
            sidecar = Path(directory_name) / "counter-42.txt"
            sidecar.write_text(
                "\n".join(
                    [
                        "clock 1000000000 1000000000",
                        "1000000000 tracked-memory 800",
                        "1100000000 tracked-memory 1000",
                        "1200000000 tracked-memory 1200",
                        "1400000000 tracked-memory 9999",
                        "1100000000 http-download 25",
                        "1200000000 http-download 50",
                        "1100000000 http-upload 10",
                        "1200000000 http-upload 20",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            profile = synthetic_profile()
            self.assertEqual(samply_profile.inject_counters(profile, [sidecar]), 3)

        self.assertEqual(profile["counters"][0], {"name": "existing"})
        memory, network_rx, network_tx = profile["counters"][1:]
        self.assertEqual(memory["mainThreadIndex"], 1)
        self.assertEqual(memory["samples"], {"timeDeltas": [100.0, 100.0], "count": [1000, 200], "length": 2})
        self.assertEqual(network_rx["samples"]["count"], [25, 50])
        self.assertNotIn("number", network_rx["samples"])
        self.assertEqual(network_rx["display"]["graphType"], "line-rate")
        self.assertEqual(network_rx["display"]["color"], "blue")
        self.assertEqual(network_tx["display"]["color"], "green")
        self.assertEqual(
            [row["format"]["unit"] for row in network_rx["display"]["tooltipRows"]],
            ["bytes-per-second", "bytes"],
        )

    def test_ignores_sidecar_for_pid_not_in_profile(self):
        with tempfile.TemporaryDirectory() as directory_name:
            sidecar = Path(directory_name) / "counter-99.txt"
            sidecar.write_text("clock 1 1\n2 tracked-memory 10\n", encoding="utf-8")
            profile = synthetic_profile()
            self.assertEqual(samply_profile.inject_counters(profile, [sidecar]), 0)
            self.assertEqual(profile["counters"], [{"name": "existing"}])

    def test_rejects_malformed_inputs_and_profiles(self):
        with tempfile.TemporaryDirectory() as directory_name:
            sidecar = Path(directory_name) / "counter-42.txt"
            sidecar.write_text("1 tracked-memory 10\n", encoding="utf-8")
            with self.assertRaisesRegex(samply_profile.ProfileInjectionError, "no clock calibration"):
                samply_profile.inject_counters(synthetic_profile(), [sidecar])
            sidecar.write_text("clock 1 1\n2 mystery 10\n", encoding="utf-8")
            with self.assertRaisesRegex(samply_profile.ProfileInjectionError, "malformed"):
                samply_profile.inject_counters(synthetic_profile(), [sidecar])

        with self.assertRaisesRegex(samply_profile.ProfileInjectionError, "meta"):
            samply_profile.inject_counters({"threads": [], "counters": []}, [])
        with self.assertRaisesRegex(samply_profile.ProfileInjectionError, "threads"):
            samply_profile.inject_counters({"meta": {"startTime": 0}, "threads": [], "counters": []}, [])

    def test_reads_gzip_and_plain_json_and_writes_gzip_atomically(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            plain = directory / "plain.json"
            compressed = directory / "compressed.json.gz"
            output = directory / "output.json.gz"
            profile = synthetic_profile()
            plain.write_text(json.dumps(profile), encoding="utf-8")
            with gzip.open(compressed, "wt", encoding="utf-8") as profile_file:
                json.dump(profile, profile_file)
            self.assertEqual(samply_profile.load_profile(plain), profile)
            self.assertEqual(samply_profile.load_profile(compressed), profile)
            samply_profile.write_profile_atomic(profile, output)
            with gzip.open(output, "rt", encoding="utf-8") as profile_file:
                self.assertEqual(json.load(profile_file), profile)
            self.assertEqual(list(directory.glob(f".{output.name}.*")), [])

    def test_injects_parallel_http_attempts_as_firefox_network_markers(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            first_sidecar = directory / "http-42-100.txt"
            second_sidecar = directory / "http-42-200.txt"
            first_sidecar.write_text(
                _http_record(
                    1100000000,
                    100000000,
                    206,
                    4096,
                    25000000,
                    "GET",
                    "https://example.com/part-1.parquet",
                    "bytes=0-4095",
                    "bytes 0-4095/8192",
                )
                + "\n"
                + _http_record(
                    1250000000,
                    10000000,
                    -1,
                    0,
                    -1,
                    "GET",
                    "https://example.com/retry.parquet",
                    "bytes=4096-8191",
                )
                + "\n",
                encoding="utf-8",
            )
            second_sidecar.write_text(
                _http_record(
                    1150000000,
                    100000000,
                    200,
                    2048,
                    50000000,
                    "HEAD",
                    "https://example.com/part-2.parquet",
                )
                + "\n",
                encoding="utf-8",
            )
            profile = synthetic_profile()
            _add_marker_table(profile["threads"][1], main=True)
            _add_marker_table(profile["threads"][2])

            self.assertEqual(samply_profile.inject_http_requests(profile, [first_sidecar, second_sidecar]), 3)

        self.assertEqual(profile["meta"]["categories"][-1]["name"], "Network")
        self.assertEqual(profile["threads"][2]["markers"]["length"], 1)
        thread = profile["threads"][1]
        self.assertEqual(thread["markers"]["length"], 7)
        self.assertEqual(
            thread["stringArray"][1:],
            [
                "Load 1 GET [Range=bytes=0-4095]: https://example.com/part-1.parquet",
                "Load 2 HEAD: https://example.com/part-2.parquet",
                "Load 3 GET [Range=bytes=4096-8191]: https://example.com/retry.parquet",
            ],
        )
        start, stop = thread["markers"]["data"][1:3]
        self.assertEqual(start["status"], "STATUS_START")
        self.assertEqual(stop["status"], "STATUS_STOP")
        self.assertEqual(stop["responseStatus"], 206)
        self.assertEqual(stop["count"], 4096)
        self.assertEqual(stop["requestRange"], "bytes=0-4095")
        self.assertEqual(stop["responseContentRange"], "bytes 0-4095/8192")
        self.assertEqual(stop["requestStart"], 100.0)
        self.assertEqual(stop["responseStart"], 125.0)
        self.assertEqual(stop["responseEnd"], 200.0)
        self.assertEqual(thread["markers"]["data"][6]["status"], "STATUS_CANCEL")

    def test_rejects_malformed_http_sidecar(self):
        with tempfile.TemporaryDirectory() as directory_name:
            sidecar = Path(directory_name) / "http-42-1.txt"
            sidecar.write_text("1\t2\t3\n", encoding="utf-8")
            with self.assertRaisesRegex(samply_profile.ProfileInjectionError, "malformed"):
                samply_profile.read_http_sidecar(sidecar)

    def test_injects_query_phase_and_operator_markers_from_profiler_output(self):
        with tempfile.TemporaryDirectory() as directory_name:
            sidecar = Path(directory_name) / "query-42-100.jsonl"
            query_profile = {
                "query": {"sql": "  SELECT\n42  "},
                "timing_bounds": [
                    {"name": "parser.total_time", "start_ns": 1_000_000, "end_ns": 3_000_000},
                    {"name": "planner.total_time", "start_ns": 3_000_000, "end_ns": 7_000_000},
                    {"name": "optimizer.total_time", "start_ns": 7_000_000, "end_ns": 9_000_000},
                    {"name": "physical_planner.total_time", "start_ns": 9_000_000, "end_ns": 10_000_000},
                ],
                "operator": [
                    {
                        "type": "TABLE_SCAN",
                        "timing": 0.004,
                        "timing_bounds": {"start_ns": 12_000_000, "end_ns": 30_000_000},
                        "extra_info": {"Table": "integers"},
                        "children": [],
                    }
                ],
            }
            sidecar.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "start_unix_ns": 1_100_000_000,
                        "duration_ns": 50_000_000,
                        "profile": query_profile,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            profile = synthetic_profile()
            _add_marker_table(profile["threads"][1], main=True)
            _add_marker_table(profile["threads"][2])
            self.assertEqual(samply_profile.inject_query_profiles(profile, [sidecar]), 8)

        thread = profile["threads"][1]
        self.assertEqual(thread["markers"]["length"], 9)
        self.assertEqual(
            thread["stringArray"][1:],
            [
                "Query: SELECT 42",
                "Planning",
                "Parser",
                "Planner",
                "Optimizer",
                "Physical Planner",
                "Execution",
                "Operator: TABLE_SCAN",
            ],
        )
        operator = thread["markers"]["data"][-1]
        self.assertEqual(operator["path"], "TABLE_SCAN")
        self.assertEqual(operator["activeDuration"], 4.0)
        self.assertEqual(thread["markers"]["startTime"][-1], 112.0)
        self.assertEqual(thread["markers"]["endTime"][-1], 130.0)
        self.assertEqual(profile["meta"]["markerSchema"][-1]["name"], "DuckDBProfile")


FAKE_SAMPLY = r"""
#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

log = Path(os.environ["FAKE_SAMPLY_LOG"])
with log.open("a", encoding="utf-8") as log_file:
    log_file.write(json.dumps({"argv": sys.argv[1:], "sidecar": os.environ.get("DUCKDB_SAMPLY_DIR")}) + "\n")

if sys.argv[1] == "load":
    raise SystemExit(int(os.environ.get("FAKE_LOAD_STATUS", "0")))

output = Path(sys.argv[sys.argv.index("--output") + 1])
if os.environ.get("FAKE_MALFORMED_PROFILE") == "1":
    output.write_text("not json", encoding="utf-8")
else:
    profile = {
        "meta": {"startTime": 1000.0, "categories": [], "markerSchema": []},
        "threads": [{"pid": 42, "processStartupTime": 0.0, "processShutdownTime": 500.0}],
        "counters": [],
    }
    output.write_text(json.dumps(profile), encoding="utf-8")
    sidecar = Path(os.environ["DUCKDB_SAMPLY_DIR"]) / "counter-42.txt"
    sidecar.write_text("clock 1000000000 1000000000\n1100000000 tracked-memory 1000\n", encoding="utf-8")
raise SystemExit(int(os.environ.get("FAKE_RECORD_STATUS", "0")))
"""


class SamplyWrapperTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary_directory.name)
        self.fake_samply = self.directory / "fake samply"
        self.fake_samply.write_text(textwrap.dedent(FAKE_SAMPLY).lstrip(), encoding="utf-8")
        self.fake_samply.chmod(self.fake_samply.stat().st_mode | stat.S_IXUSR)
        self.log = self.directory / "fake.log"
        self.output = self.directory / "final profile.json.gz"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def environment(self, **extra):
        return mock.patch.dict(os.environ, {"FAKE_SAMPLY_LOG": str(self.log), **extra}, clear=False)

    def log_entries(self):
        return [json.loads(line) for line in self.log.read_text(encoding="utf-8").splitlines()]

    def test_record_arguments_environment_and_cleanup(self):
        with self.environment():
            status = samply_profile.main(
                [
                    "--samply",
                    str(self.fake_samply),
                    "--output",
                    str(self.output),
                    "--rate",
                    "250",
                    "--duration",
                    "2.5",
                    "--profile-name",
                    "demo",
                    "--no-open",
                    "--",
                    "/bin/echo",
                    "; not shell syntax",
                ]
            )
        self.assertEqual(status, 0)
        entry = self.log_entries()[0]
        self.assertEqual(entry["argv"][0:2], ["record", "--save-only"])
        self.assertIn("250", entry["argv"])
        self.assertEqual(entry["argv"][-3:], ["--", "/bin/echo", "; not shell syntax"])
        self.assertFalse(Path(entry["sidecar"]).exists())
        with gzip.open(self.output, "rt", encoding="utf-8") as profile_file:
            self.assertEqual(json.load(profile_file)["counters"][0]["name"], "Tracked Memory")

    def test_load_is_run_and_failure_is_reported(self):
        with self.environment(FAKE_LOAD_STATUS="9"):
            status = samply_profile.main(
                ["--samply", str(self.fake_samply), "--output", str(self.output), "--", "/bin/true"]
            )
        self.assertEqual(status, 9)
        entries = self.log_entries()
        self.assertEqual(entries[1]["argv"], ["load", str(self.output)])
        sidecar_directory = Path(entries[0]["sidecar"])
        self.assertTrue(sidecar_directory.exists())
        shutil.rmtree(sidecar_directory)

    def test_record_status_is_preserved(self):
        with self.environment(FAKE_RECORD_STATUS="7"):
            status = samply_profile.main(
                ["--samply", str(self.fake_samply), "--output", str(self.output), "--no-open", "--", "/bin/false"]
            )
        self.assertEqual(status, 7)
        sidecar_directory = Path(self.log_entries()[0]["sidecar"])
        self.assertTrue(sidecar_directory.exists())
        shutil.rmtree(sidecar_directory)

    def test_injection_failure_preserves_raw_profile_and_sidecars(self):
        with self.environment(FAKE_MALFORMED_PROFILE="1"):
            status = samply_profile.main(
                ["--samply", str(self.fake_samply), "--output", str(self.output), "--no-open", "--", "/bin/true"]
            )
        self.assertEqual(status, 1)
        sidecar_directory = Path(self.log_entries()[0]["sidecar"])
        self.assertTrue((sidecar_directory / "raw-profile.json.gz").exists())
        shutil.rmtree(sidecar_directory)


class DuckDBLauncherTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary_directory.name)
        self.launcher = self.directory / "profile"
        self.duckdb = self.directory / "duckdb"
        self.duckdb.touch()
        self.resolved_duckdb = self.duckdb.resolve()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_constructs_default_command(self):
        self.assertEqual(
            samply_profile.duckdb_arguments([], self.launcher),
            ["--", str(self.resolved_duckdb), "-cmd", "SET samply_tracks='all';"],
        )

    def test_forwards_all_arguments_to_duckdb_without_separator(self):
        self.assertEqual(
            samply_profile.duckdb_arguments(["database.db", "-c", "SELECT 42"], self.launcher),
            [
                "--",
                str(self.resolved_duckdb),
                "-cmd",
                "SET samply_tracks='all';",
                "database.db",
                "-c",
                "SELECT 42",
            ],
        )

    def test_splits_wrapper_options_from_duckdb_arguments(self):
        self.assertEqual(
            samply_profile.duckdb_arguments(
                ["--no-open", "--rate", "100", "--", "database.db", "-c", "SELECT 42"], self.launcher
            ),
            [
                "--no-open",
                "--rate",
                "100",
                "--",
                str(self.resolved_duckdb),
                "-cmd",
                "SET samply_tracks='all';",
                "database.db",
                "-c",
                "SELECT 42",
            ],
        )

    def test_preserves_option_like_duckdb_arguments(self):
        self.assertEqual(
            samply_profile.duckdb_arguments(["--no-open", "--output", "query.csv"], self.launcher),
            [
                "--",
                str(self.resolved_duckdb),
                "-cmd",
                "SET samply_tracks='all';",
                "--no-open",
                "--output",
                "query.csv",
            ],
        )

    def test_explicit_track_setting_can_override_default(self):
        self.assertEqual(
            samply_profile.duckdb_arguments(["-cmd", "SET samply_tracks='http';", "-f", "query.sql"], self.launcher),
            [
                "--",
                str(self.resolved_duckdb),
                "-cmd",
                "SET samply_tracks='all';",
                "-cmd",
                "SET samply_tracks='http';",
                "-f",
                "query.sql",
            ],
        )

    def test_resolves_duckdb_next_to_launcher(self):
        nested_launcher = self.directory / "nested" / ".." / "profile"
        self.assertEqual(samply_profile.resolve_duckdb(nested_launcher), self.resolved_duckdb)

    def test_reports_missing_duckdb(self):
        self.duckdb.unlink()
        with self.assertRaisesRegex(
            samply_profile.ProfileInjectionError,
            "could not find DuckDB executable next to profile launcher",
        ):
            samply_profile.resolve_duckdb(self.launcher)


if __name__ == "__main__":
    unittest.main()
