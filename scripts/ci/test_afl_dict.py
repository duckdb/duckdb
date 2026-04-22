#!/usr/bin/env python3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import afl_dict


class FuzzDictTest(unittest.TestCase):
    def test_run_forwards_target_command_args(self):
        with tempfile.TemporaryDirectory(prefix="fuzz_dict_test_") as tmpdir:
            tmp_root = Path(tmpdir)
            input_dir = tmp_root / "input"
            input_dir.mkdir(parents=True, exist_ok=True)
            (input_dir / "seed1.test").write_text("statement ok\nSELECT 42;\n", encoding="utf8")

            output_file = tmp_root / "dict.txt"
            fake_target = tmp_root / "fake_target"
            fake_target.write_text("fake target", encoding="utf8")

            def fake_afl_fuzz_call(cmd, text, capture_output, check, env):
                self.assertTrue(text)
                self.assertTrue(capture_output)
                self.assertFalse(check)
                self.assertIsInstance(env, dict)
                self.assertIn("--writable-dir", cmd)
                self.assertIn(str(tmp_root / "out"), cmd)
                self.assertEqual(cmd[cmd.index("--") + 1], str(fake_target))
                self.assertEqual(cmd[-2], "--writable-dir")
                self.assertEqual(cmd[-1], str(tmp_root / "out"))

                out_root = Path(cmd[cmd.index("-o") + 1])
                token_dir = out_root / "default" / ".state" / "auto_extras"
                token_dir.mkdir(parents=True, exist_ok=True)
                (token_dir / "auto_000001").write_bytes(b"SELECT")
                return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

            config = afl_dict.DictConfig(
                input_dir=input_dir,
                output_file=output_file,
                target=f"{fake_target} --writable-dir {tmp_root / 'out'}",
                fuzz_secs=1,
            )

            with (
                mock.patch("scripts.ci.afl_dict.shutil.which", return_value="/usr/bin/fake-afl-fuzz"),
                mock.patch("scripts.ci.afl_dict.subprocess.run", side_effect=fake_afl_fuzz_call),
            ):
                rc = afl_dict.run(config)

            self.assertEqual(rc, 0)
            self.assertTrue(output_file.exists())
            contents = output_file.read_text(encoding="utf8")
            self.assertIn('tok_000000="SELECT"', contents)


if __name__ == "__main__":
    unittest.main()
