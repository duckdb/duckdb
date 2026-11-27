#!/usr/bin/env python3

import contextlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import check_extension_patches as checker


class ExtensionPatchCheckTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config_dir = self.root / ".github" / "config" / "extensions"
        self.config_dir.mkdir(parents=True)
        self.env = mock.patch.dict(
            os.environ,
            {
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_ALLOW_PROTOCOL": "file",
                "GIT_TERMINAL_PROMPT": "0",
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)

    def write_config(self, name, url="unused", tag="pin", apply_patches=True, submodules=()):
        config = self.config_dir / f"{name}.cmake"
        arguments = f'GIT_URL "{url}" GIT_TAG "{tag}"'
        if apply_patches:
            arguments += " APPLY_PATCHES"
        if submodules:
            arguments += " SUBMODULES " + " ".join(f'"{submodule}"' for submodule in submodules)
        config.write_text(f"duckdb_extension_load({name} {arguments})\n", encoding="utf8")
        return config

    def select(self, *files):
        return checker.select_extensions(self.root, files, checker.collect_patched_extensions(self.root))

    def git(self, repo, *arguments):
        return subprocess.check_output(
            [
                "git",
                "-C",
                str(repo),
                "-c",
                "user.name=Patch Check Test",
                "-c",
                "user.email=patch-check@example.com",
                *arguments,
            ],
            stderr=subprocess.PIPE,
            text=True,
        ).strip()

    def make_remote(self, name="example", filename="source.txt"):
        remote = self.root / "remotes" / name
        remote.mkdir(parents=True)
        self.git(remote, "init", "--quiet")
        (remote / filename).write_text("original\n", encoding="utf8")
        self.git(remote, "add", ".")
        self.git(remote, "commit", "--quiet", "-m", "initial")
        return remote

    def write_patch(self, name, before, after, filename="source.txt"):
        directory = self.root / ".github" / "patches" / "extensions" / "example"
        directory.mkdir(parents=True, exist_ok=True)
        patch = directory / name
        patch.write_text(
            f"diff --git a/{filename} b/{filename}\n"
            f"--- a/{filename}\n"
            f"+++ b/{filename}\n"
            "@@ -1 +1 @@\n"
            f"-{before}\n"
            f"+{after}\n",
            encoding="utf8",
        )
        return patch

    def run_check(self, *files):
        changed_files = self.root / "changed-files.txt"
        changed_files.write_text("\n".join(files), encoding="utf8")
        output, errors = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(checker, "REPO_ROOT", self.root),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            result = checker.main(["--changed-files", str(changed_files)])
        return result, output.getvalue() + errors.getvalue()

    def test_patch_paths_select_only_the_named_extension(self):
        self.write_config("example")
        self.write_config("unaffected")
        extensions = self.select(".github/patches/extensions/example/deleted.patch")
        self.assertEqual([extension["name"] for extension in extensions], ["example"])

    def test_individual_config_selects_its_declared_extensions(self):
        config = self.write_config("example")
        self.write_config("dependency")
        self.write_config("unaffected")
        with config.open("a", encoding="utf8") as file:
            file.write('include("${EXTENSION_CONFIG_BASE_DIR}/dependency.cmake")\n')
        extensions = self.select(".github/config/extensions/example.cmake")
        self.assertEqual({extension["name"] for extension in extensions}, {"example", "dependency"})

    def test_shared_or_unknown_paths_select_all_patched_extensions(self):
        self.write_config("example")
        self.write_config("unaffected")
        for path in (
            ".github/config/cloud.cmake",
            ".github/patches/extensions/README.md",
            ".github/config/unknown.txt",
        ):
            with self.subTest(path=path):
                extensions = self.select(path)
                self.assertEqual({extension["name"] for extension in extensions}, {"example", "unaffected"})

    def test_removed_configs_and_unpatched_extensions_are_skipped(self):
        self.write_config("example", apply_patches=False)
        self.write_config("removed").unlink()
        self.write_config("unaffected")
        self.assertEqual(
            self.select(
                ".github/config/extensions/example.cmake",
                ".github/config/extensions/removed.cmake",
                ".github/patches/extensions/removed/deleted.patch",
            ),
            [],
        )

    def test_duplicate_declarations_are_deduplicated_and_distinct_pins_retained(self):
        self.write_config("example")
        (self.config_dir.parent / "group.cmake").write_text(
            'include("${EXTENSION_CONFIG_BASE_DIR}/example.cmake")\n'
            'duckdb_extension_load(example GIT_URL unused GIT_TAG other APPLY_PATCHES)\n',
            encoding="utf8",
        )
        extensions = self.select(".github/config/extensions/example.cmake")
        self.assertEqual({extension["git_tag"] for extension in extensions}, {"pin", "other"})
        self.assertEqual(len(extensions), 2)

    def test_conditionally_disabled_extensions_are_checked(self):
        config = self.write_config("example")
        config.write_text(f"if(OFF)\n{config.read_text(encoding='utf8')}endif()\n", encoding="utf8")
        self.assertEqual(len(self.select(".github/config/extensions/example.cmake")), 1)

    @unittest.skipUnless(shutil.which("git") and shutil.which("patch"), "requires Git and patch")
    def test_dependent_patches_apply_in_filename_order(self):
        remote = self.make_remote()
        self.write_config("example", remote.as_uri(), self.git(remote, "rev-parse", "HEAD"))
        self.write_patch("0002-second.patch", "first", "second")
        self.write_patch("0001-first.patch", "original", "first")
        result, output = self.run_check(".github/patches/extensions/example/0002-second.patch")
        self.assertEqual(result, 0, output)
        self.assertLess(
            output.index("Applying patch: 0001-first.patch"), output.index("Applying patch: 0002-second.patch")
        )
        self.assertEqual((remote / "source.txt").read_text(encoding="utf8"), "original\n")

    @unittest.skipUnless(shutil.which("git") and shutil.which("patch"), "requires Git and patch")
    def test_conflicting_patches_fail_even_with_local_skip_setting(self):
        remote = self.make_remote()
        self.write_config("example", remote.as_uri(), self.git(remote, "rev-parse", "HEAD"))
        self.write_patch("0001-first.patch", "original", "first")
        self.write_patch("0002-conflicting.patch", "original", "conflicting")
        with mock.patch.dict(os.environ, {"DUCKDB_SKIP_APPLYING_PATCHES": "1"}):
            result, output = self.run_check(".github/patches/extensions/example/0002-conflicting.patch")
        self.assertEqual(result, 1, output)
        self.assertIn("Checking patches for example", output)
        self.assertIn("0002-conflicting.patch", output)
        self.assertIn("Failed to apply patch", output)

    @unittest.skipUnless(shutil.which("git") and shutil.which("patch"), "requires Git and patch")
    def test_moved_patch_checks_the_source_extensions_remaining_stack(self):
        remote = self.make_remote()
        pin = self.git(remote, "rev-parse", "HEAD")
        self.write_config("example", remote.as_uri(), pin)
        self.write_config("moved", remote.as_uri(), pin)
        patch = self.write_patch("0001-first.patch", "original", "first")
        self.write_patch("0002-second.patch", "first", "second")
        destination = patch.parent.parent / "moved" / patch.name
        destination.parent.mkdir()
        patch.rename(destination)
        result, output = self.run_check(
            ".github/patches/extensions/example/0001-first.patch",
            ".github/patches/extensions/moved/0001-first.patch",
        )
        self.assertEqual(result, 1, output)
        self.assertIn("Checking patches for example", output)
        self.assertIn("0002-second.patch", output)

    @unittest.skipUnless(shutil.which("git") and shutil.which("patch"), "requires Git and patch")
    def test_checks_the_pin_instead_of_the_remote_head(self):
        remote = self.make_remote()
        self.write_config("example", remote.as_uri(), self.git(remote, "rev-parse", "HEAD"))
        self.write_patch("0001-change.patch", "original", "patched")
        (remote / "source.txt").write_text("upstream change\n", encoding="utf8")
        self.git(remote, "commit", "--quiet", "-am", "upstream change")
        result, output = self.run_check(".github/config/extensions/example.cmake")
        self.assertEqual(result, 0, output)

    @unittest.skipUnless(shutil.which("git") and shutil.which("patch"), "requires Git and patch")
    def test_revision_bump_checks_the_new_pin(self):
        remote = self.make_remote()
        self.write_config("example", remote.as_uri(), self.git(remote, "rev-parse", "HEAD"))
        self.write_patch("0001-change.patch", "original", "patched")
        result, output = self.run_check(".github/config/extensions/example.cmake")
        self.assertEqual(result, 0, output)
        (remote / "source.txt").write_text("upstream change\n", encoding="utf8")
        self.git(remote, "commit", "--quiet", "-am", "upstream change")
        pin = self.git(remote, "rev-parse", "HEAD")
        self.write_config("example", remote.as_uri(), pin)
        result, output = self.run_check(".github/config/extensions/example.cmake")
        self.assertEqual(result, 1, output)
        self.assertIn(f"example @ {pin}", output)
        self.assertIn("0001-change.patch", output)

    @unittest.skipUnless(shutil.which("git") and shutil.which("patch"), "requires Git and patch")
    def test_patches_inside_configured_submodules_apply(self):
        library = self.make_remote("library", "library.txt")
        remote = self.make_remote()
        self.git(remote, "submodule", "add", library.as_uri(), "third_party/library")
        self.git(remote, "commit", "--quiet", "-am", "add submodule")
        self.write_config(
            "example", remote.as_uri(), self.git(remote, "rev-parse", "HEAD"), submodules=["third_party/library"]
        )
        self.write_patch("0001-library.patch", "original", "patched", filename="third_party/library/library.txt")
        result, output = self.run_check(".github/config/extensions/example.cmake")
        self.assertEqual(result, 0, output)
        self.assertIn("Applying patch: 0001-library.patch", output)

    @unittest.skipUnless(shutil.which("git"), "requires Git")
    def test_fetch_failure_reports_the_extension_and_revision(self):
        remote = self.make_remote()
        self.write_config("example", remote.as_uri(), "missing-revision")
        result, output = self.run_check(".github/config/extensions/example.cmake")
        self.assertEqual(result, 1, output)
        self.assertIn("example @ missing-revision", output)
        self.assertIn("git fetch", output)

    def test_removing_the_last_patch_requires_no_checkout(self):
        self.write_config("example", apply_patches=False)
        result, output = self.run_check(".github/patches/extensions/example/deleted.patch")
        self.assertEqual(result, 0, output)
        self.assertIn("0 extension declarations", output)

    def test_missing_changed_files_list_fails(self):
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors):
            result = checker.main(["--changed-files", str(self.root / "missing.txt")])
        self.assertEqual(result, 1)
        self.assertIn("missing.txt", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
