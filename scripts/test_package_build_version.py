import os
import unittest
from unittest.mock import patch

import package_build


class TestPackageBuildVersion(unittest.TestCase):
    def test_local_development_version(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch('package_build.git_commit_count', return_value='42'):
                self.assertEqual(package_build.git_dev_version(), 'v2.0.0-dev42')

    def test_explicit_version(self):
        with patch.dict(os.environ, {'DUCKDB_VERSION': 'v2.0.0-alpha1'}, clear=True):
            self.assertEqual(package_build.git_dev_version(), 'v2.0.0-alpha1')

    def test_legacy_explicit_version(self):
        with patch.dict(os.environ, {'OVERRIDE_GIT_DESCRIBE': 'v2.0.0-rc1'}, clear=True):
            self.assertEqual(package_build.git_dev_version(), 'v2.0.0-rc1')

    def test_duckdb_version_takes_precedence(self):
        environment = {
            'DUCKDB_VERSION': 'v2.1.0-dev42',
            'OVERRIDE_GIT_DESCRIBE': 'v2.0.0-rc1',
        }
        with patch.dict(os.environ, environment, clear=True):
            self.assertEqual(package_build.git_dev_version(), 'v2.1.0-dev42')

    def test_explicit_commit_is_truncated(self):
        with patch.dict(os.environ, {'DUCKDB_COMMIT': 'abcdef0123456789'}, clear=True):
            self.assertEqual(package_build.git_commit_hash(), 'abcdef0123')

    def test_local_commit_uses_full_hash(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch('subprocess.check_output', return_value='abcdef0123456789\n') as check_output:
                self.assertEqual(package_build.git_commit_hash(), 'abcdef0123')
                check_output.assert_called_once_with(['git', 'log', '-1', '--format=%H'], text=True)


if __name__ == '__main__':
    unittest.main()
