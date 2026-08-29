#!/usr/bin/env python3

import io
import stat
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import check_staged_extensions


def create_tarball(members: list[tuple[str, bytes, int]]) -> bytes:
    archive_buffer = io.BytesIO()
    with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
        for name, contents, mode in members:
            member = tarfile.TarInfo(name)
            member.size = len(contents)
            member.mode = mode
            archive.addfile(member, io.BytesIO(contents))
    return archive_buffer.getvalue()


class CheckStagedExtensionsTest(unittest.TestCase):
    def test_cli_asset_names(self):
        self.assertEqual(
            check_staged_extensions.cli_asset_name("Linux", "x86_64"),
            "duckdb-cli-linux-amd64.tar.gz",
        )
        self.assertEqual(
            check_staged_extensions.cli_asset_name("Darwin", "arm64"),
            "duckdb-cli-osx-arm64.tar.gz",
        )

    def test_extract_cli(self):
        archive = create_tarball([("duckdb", b"binary", 0o755)])
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "duckdb"
            check_staged_extensions.extract_cli(archive, target)

            self.assertEqual(target.read_bytes(), b"binary")
            self.assertTrue(target.stat().st_mode & stat.S_IXUSR)

    def test_extract_cli_rejects_nested_member(self):
        archive = create_tarball([("directory/duckdb", b"binary", 0o755)])
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "only a top-level duckdb file"):
                check_staged_extensions.extract_cli(archive, Path(temp_dir) / "duckdb")

    def test_extract_cli_rejects_additional_members(self):
        archive = create_tarball(
            [
                ("duckdb", b"binary", 0o755),
                ("unexpected", b"contents", 0o644),
            ]
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "only a top-level duckdb file"):
                check_staged_extensions.extract_cli(archive, Path(temp_dir) / "duckdb")


if __name__ == "__main__":
    unittest.main()
