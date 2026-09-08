#!/usr/bin/env python3

import os
import stat
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


class PackageReleaseArtifactTest(unittest.TestCase):
    def run_make(self, target: str, output_dir: Path, **variables: str) -> None:
        environment = os.environ.copy()
        environment["ARTIFACT_OUTPUT_DIR"] = str(output_dir)
        command = ["make", target]
        command.extend(f"{name}={value}" for name, value in variables.items())
        subprocess.run(command, cwd=REPO_ROOT, env=environment, check=True, capture_output=True, text=True)

    def test_cli_release_artifact(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            cli = root / "duckdb"
            cli.write_bytes(b"binary")
            cli.chmod(0o755)

            self.run_make(
                "cli-release-artifact",
                root,
                ARTIFACT_SUFFIX="linux-amd64",
                CLI_BINARY=str(cli),
            )

            archive_path = root / "duckdb-cli-linux-amd64.tar.gz"
            with tarfile.open(archive_path, mode="r:gz") as archive:
                members = archive.getmembers()
                self.assertEqual([member.name for member in members], ["duckdb"])
                self.assertTrue(members[0].mode & stat.S_IXUSR)
                self.assertEqual(archive.extractfile(members[0]).read(), b"binary")

    def test_shared_libraries_release_artifact(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            versioned_library = root / "libduckdb.so.1"
            versioned_library.write_bytes(b"library")
            library_link = root / "libduckdb.so"
            library_link.symlink_to(versioned_library.name)

            self.run_make(
                "shared-libs-release-artifact",
                root,
                ARTIFACT_SUFFIX="linux-amd64",
                SHARED_LIBRARIES=f"{library_link} {versioned_library}",
            )

            archive_path = root / "duckdb-shared-libs-linux-amd64.tar.gz"
            with tarfile.open(archive_path, mode="r:gz") as archive:
                members = {member.name: member for member in archive.getmembers()}
                self.assertEqual(
                    set(members),
                    {
                        "libduckdb.so",
                        "libduckdb.so.1",
                        "duckdb.h",
                        "duckdb_v2.h",
                        "duckdb_extension.h",
                        "duckdb_extension_v2.h",
                    },
                )
                self.assertTrue(members["libduckdb.so"].issym())
                self.assertEqual(members["libduckdb.so"].linkname, "libduckdb.so.1")


if __name__ == "__main__":
    unittest.main()
