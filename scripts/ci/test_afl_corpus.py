#!/usr/bin/env python3
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import afl_corpus as corpus


class FuzzCorpusTest(unittest.TestCase):
    def test_run_optimizer_glob_with_fake_afl_cmin(self):
        test_glob = "test/optimizer/**/*.test"
        test_files = corpus.list_test_files(test_glob)
        self.assertGreater(len(test_files), 0, f"expected files for glob {test_glob}")
        expected_groups = {corpus.group_name_for(path) for path in test_files}
        self.assertGreater(len(expected_groups), 0)

        with tempfile.TemporaryDirectory(prefix="fuzz_corpus_test_") as tmpdir:
            tmp_root = Path(tmpdir)
            output_dir = tmp_root / "output"
            fake_target = tmp_root / "fake_target"
            fake_target.write_text("fake target", encoding="utf8")

            called_groups: set[str] = set()
            lock = threading.Lock()

            def fake_afl_cmin_call(cmd, text, capture_output, check):
                self.assertTrue(text)
                self.assertTrue(capture_output)
                self.assertFalse(check)
                self.assertGreaterEqual(len(cmd), 7)

                input_dir = Path(cmd[cmd.index("-i") + 1])
                output_group_dir = Path(cmd[cmd.index("-o") + 1])
                target = Path(cmd[-1])

                self.assertEqual(cmd[0], "fake-afl-cmin")
                self.assertEqual(target, fake_target)

                output_group_dir.mkdir(parents=True, exist_ok=True)
                source_files = sorted(path for path in input_dir.iterdir() if path.is_file())
                self.assertGreater(len(source_files), 0)
                source = source_files[0]
                shutil.copy2(source, output_group_dir / source.name)

                with lock:
                    called_groups.add(input_dir.name)

                return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

            config = corpus.CorpusConfig(
                output_dir=output_dir,
                glob_pattern=test_glob,
                jobs=2,
                target=fake_target,
                afl_cmin_bin="fake-afl-cmin",
            )

            with (
                mock.patch("scripts.ci.afl_corpus.shutil.which", return_value="/usr/bin/fake-afl-cmin"),
                mock.patch("scripts.ci.afl_corpus.subprocess.run", side_effect=fake_afl_cmin_call) as run_mock,
            ):
                rc = corpus.run(config)

            self.assertEqual(rc, 0)
            self.assertEqual(run_mock.call_count, len(expected_groups))
            self.assertSetEqual(called_groups, expected_groups)

            output_entries = sorted(output_dir.iterdir())
            self.assertGreater(len(output_entries), 0)
            self.assertTrue(all(path.is_file() for path in output_entries))
            self.assertTrue(all(path.suffix == ".test" for path in output_entries))
            self.assertTrue(all(path.name.startswith("optimizer") for path in output_entries))


if __name__ == "__main__":
    unittest.main()
