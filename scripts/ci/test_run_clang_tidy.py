#!/usr/bin/env python3

import importlib.util
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "scripts" / "run-clang-tidy.py"
SPEC = importlib.util.spec_from_file_location("run_clang_tidy", RUNNER_PATH)
RUN_CLANG_TIDY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUN_CLANG_TIDY)


class RunClangTidyTest(unittest.TestCase):
    def test_shards_are_deterministic_disjoint_and_complete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir)
            files = [source_root / "src" / f"file_{index}.cpp" for index in range(100)]
            third_party_path = source_root / "third_party"
            file_name_re = re.compile(".*")

            shards = []
            for shard_index in range(4):
                selected, eligible_count = RUN_CLANG_TIDY.select_files_for_shard(
                    [str(path) for path in files],
                    str(source_root),
                    str(third_party_path),
                    file_name_re,
                    shard_index,
                    4,
                )
                self.assertEqual(eligible_count, len(files))
                self.assertEqual(
                    selected,
                    RUN_CLANG_TIDY.select_files_for_shard(
                        [str(path) for path in files],
                        str(source_root),
                        str(third_party_path),
                        file_name_re,
                        shard_index,
                        4,
                    )[0],
                )
                shards.append(set(selected))

            for left_index, left_shard in enumerate(shards):
                for right_shard in shards[left_index + 1 :]:
                    self.assertFalse(left_shard & right_shard)
            self.assertEqual(set().union(*shards), {str(path) for path in files})

    def test_filters_third_party_and_non_matching_files_before_sharding(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir)
            files = [
                source_root / "src" / "included.cpp",
                source_root / "src" / "excluded.c",
                source_root / "third_party" / "dependency.cpp",
            ]

            selected, eligible_count = RUN_CLANG_TIDY.select_files_for_shard(
                [str(path) for path in files],
                str(source_root),
                str(source_root / "third_party"),
                re.compile(r"\.cpp$"),
                0,
                1,
            )

            self.assertEqual(selected, [str(files[0])])
            self.assertEqual(eligible_count, 1)

    def test_validates_shard_bounds(self):
        RUN_CLANG_TIDY.validate_shard(0, 1)
        RUN_CLANG_TIDY.validate_shard(3, 4)
        with self.assertRaisesRegex(ValueError, "at least 1"):
            RUN_CLANG_TIDY.validate_shard(0, 0)
        with self.assertRaisesRegex(ValueError, "between 0"):
            RUN_CLANG_TIDY.validate_shard(-1, 4)
        with self.assertRaisesRegex(ValueError, "between 0"):
            RUN_CLANG_TIDY.validate_shard(4, 4)

    @unittest.skipIf(sys.platform == "win32", "requires executable Python scripts")
    def test_runs_32_concurrent_workers_and_preserves_failures(self):
        for exit_code in (0, 1):
            with self.subTest(exit_code=exit_code), tempfile.TemporaryDirectory() as temp_dir:
                build_path = Path(temp_dir)
                files = [build_path / f"file_{index}.cpp" for index in range(64)]
                database = [{"file": str(path), "directory": temp_dir} for path in files]
                (build_path / "compile_commands.json").write_text(json.dumps(database))
                fake_tidy = build_path / "clang-tidy"
                fake_tidy.write_text(
                    f"""#!{sys.executable}
import sys
import time
from pathlib import Path

if '-list-checks' in sys.argv:
    sys.exit(0)
source = Path(sys.argv[-1])
source.with_suffix('.started').touch()
deadline = time.monotonic() + 15
while len(list(source.parent.glob('*.started'))) < 32:
    if time.monotonic() > deadline:
        sys.exit(2)
    time.sleep(0.01)
sys.exit({exit_code} if source.stem == 'file_63' else 0)
"""
                )
                fake_tidy.chmod(0o755)

                result = subprocess.run(
                    [
                        sys.executable,
                        str(RUNNER_PATH),
                        "-p",
                        temp_dir,
                        "-clang-tidy-binary",
                        str(fake_tidy),
                        "-quiet",
                        "-j",
                        "32",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )

                self.assertEqual(result.returncode, exit_code, result.stdout + result.stderr)
                self.assertIn("on 64 of 64 files with 32 parallel workers", result.stdout)
                self.assertIn("Peak concurrent clang-tidy workers: 32/32", result.stdout)
                self.assertEqual(len(list(build_path.glob("*.started"))), len(files))


if __name__ == "__main__":
    unittest.main()
