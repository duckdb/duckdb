#!/usr/bin/env python3

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import sync_out_of_tree_extensions as sync

MERGE_SCRIPT = REPO_ROOT / 'scripts/merge_vcpkg_deps.py'


class VcpkgManifestOpenSSLTest(unittest.TestCase):
    def test_legacy_merger_uses_baseline_openssl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / 'vcpkg.json'
            manifest.write_text(json.dumps({'dependencies': ['example']}))
            output = root / 'build/extension_configuration/vcpkg.json'
            output.parent.mkdir(parents=True)

            result = subprocess.run(
                [sys.executable, str(MERGE_SCRIPT), str(manifest)],
                cwd=root,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn('overrides', json.loads(output.read_text()))

    def test_out_of_tree_merger_uses_baseline_openssl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            external_dir = root / 'external'
            extension_dir = external_dir / 'example'
            extension_dir.mkdir(parents=True)
            (extension_dir / 'vcpkg.json').write_text(json.dumps({'dependencies': ['example']}))
            output_dir = root / 'build'

            with patch.dict(os.environ, {'VCPKG_TOOLCHAIN_PATH': '/unused/vcpkg.cmake'}, clear=True):
                sync.merge_vcpkg_manifests(['example'], external_dir, root, output_dir)

            self.assertNotIn('overrides', json.loads((output_dir / 'vcpkg.json').read_text()))


if __name__ == '__main__':
    unittest.main()
