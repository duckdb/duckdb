#!/usr/bin/env python3

import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci.merge_extension_artifacts import merge_extension_artifacts


class MergeExtensionArtifactsTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        self.repository = self.root / "repository"

    def add_file(self, artifact, relative="revision/linux_amd64/icu.duckdb_extension", contents=b"extension"):
        path = self.artifacts / artifact / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
        return path

    def merge(self):
        with contextlib.redirect_stdout(io.StringIO()):
            merge_extension_artifacts(self.artifacts, self.repository)

    def test_dedicated_build_overrides_release_core_and_preserves_fallbacks(self):
        primary = self.add_file("core-extensions-linux-amd64", contents=b"dedicated build")
        self.add_file("release-core-extensions-linux-amd64", contents=b"release build with a different length")
        fallback = self.add_file(
            "release-core-extensions-linux-amd64", "revision/linux_amd64/tpch.duckdb_extension", b"fallback"
        )
        windows = self.add_file(
            "release-core-extensions-windows-amd64", "revision/windows_amd64/icu.duckdb_extension", b"windows"
        )
        wasm = self.add_file("core-extensions-wasm-eh", "revision/wasm_eh/icu.duckdb_extension.wasm", b"wasm")
        cloud = self.add_file("cloud-extensions-linux-amd64", "revision/linux_amd64/httpfs.duckdb_extension", b"httpfs")
        self.merge()

        for source in (primary, fallback, windows, wasm, cloud):
            relative = Path(*source.relative_to(self.artifacts).parts[1:])
            self.assertEqual((self.repository / relative).read_bytes(), source.read_bytes())
        self.assertEqual(len(list(self.repository.rglob("*.duckdb_extension*"))), 5)

    def test_shared_dependency_builds_use_artifact_order(self):
        relative = "revision/linux_amd64/waddle.duckdb_extension"
        # Create directories in reverse order to check that enumeration order does not choose the winner.
        selected = self.add_file("core-extensions-linux-amd64", relative, b"core build")
        self.add_file("cloud-extensions-linux-amd64", relative, b"cloud build with a different length")
        self.merge()
        self.assertEqual((self.repository / relative).read_bytes(), selected.read_bytes())

    def test_duplicate_release_builds_use_artifact_order(self):
        self.add_file("release-core-extensions-other-build", contents=b"other")
        self.add_file("release-core-extensions-linux-amd64", contents=b"first build with a different length")
        self.merge()
        self.assertEqual((self.repository / "revision/linux_amd64/icu.duckdb_extension").read_bytes(), b"other")

    def test_identical_duplicates_are_allowed(self):
        source = self.add_file("core-extensions-linux-amd64")
        self.add_file("other-extensions-linux-amd64")
        self.merge()
        self.assertEqual(
            (self.repository / "revision/linux_amd64/icu.duckdb_extension").read_bytes(), source.read_bytes()
        )

    def test_empty_input_fails(self):
        with self.assertRaisesRegex(ValueError, "No extension artifact files"):
            self.merge()
        self.assertFalse(self.repository.exists())

    def test_existing_repository_is_not_modified(self):
        self.add_file("core-extensions-linux-amd64")
        self.repository.mkdir()
        existing = self.repository / "existing"
        existing.write_bytes(b"keep")
        with self.assertRaises(FileExistsError):
            self.merge()
        self.assertEqual(existing.read_bytes(), b"keep")
        self.assertEqual(list(self.repository.iterdir()), [existing])

    def test_symlink_is_rejected(self):
        source = self.add_file("core-extensions-linux-amd64")
        source.with_suffix(".link").symlink_to(source)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.merge()
        self.assertFalse(self.repository.exists())

    def test_command_line(self):
        source = self.add_file("release-core-extensions-linux-amd64")
        result = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts/ci/merge_extension_artifacts.py"),
                str(self.artifacts),
                str(self.repository),
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (self.repository / "revision/linux_amd64/icu.duckdb_extension").read_bytes(), source.read_bytes()
        )


if __name__ == "__main__":
    unittest.main()
