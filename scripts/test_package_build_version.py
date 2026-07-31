import os
import unittest
from unittest.mock import patch

import package_build


class TestPackageBuildVersion(unittest.TestCase):
    def test_release_tag_version(self):
        with patch('package_build.get_git_describe', return_value='v1.5.0-0-gabc123'):
            self.assertEqual(package_build.git_dev_version(), 'v1.5.0')
            self.assertEqual(package_build.git_commit_hash(), 'abc123')

    def test_dev_version_from_release_tag(self):
        with patch('package_build.get_git_describe', return_value='v1.5.0-42-gabc123'):
            self.assertEqual(package_build.git_dev_version(), 'v1.6.0-dev42')
            self.assertEqual(package_build.git_commit_hash(), 'abc123')

    def test_alpha_tag_version(self):
        with patch('package_build.get_git_describe', return_value='v2.0.0-alpha35494-0-gabc123'):
            self.assertEqual(package_build.git_dev_version(), 'v2.0.0-alpha35494')
            self.assertEqual(package_build.git_commit_hash(), 'abc123')

    def test_dev_version_from_alpha_tag(self):
        with patch('package_build.get_git_describe', return_value='v2.0.0-alpha1-42-gabc123'):
            self.assertEqual(package_build.git_dev_version(), 'v2.0.0-dev42')
            self.assertEqual(package_build.git_commit_hash(), 'abc123')

    def test_prerelease_git_describe_override(self):
        with patch.dict(os.environ, {'OVERRIDE_GIT_DESCRIBE': 'v2.0.0-alpha1-42-gabc123'}):
            self.assertEqual(package_build.get_git_describe(), 'v2.0.0-alpha1-42-gabc123')

    def test_rc_tag_version(self):
        with patch('package_build.get_git_describe', return_value='v2.0.0-rc1-0-gabc123'):
            self.assertEqual(package_build.git_dev_version(), 'v2.0.0-rc1')
            self.assertEqual(package_build.git_commit_hash(), 'abc123')

    def test_dev_version_from_rc_tag(self):
        with patch('package_build.get_git_describe', return_value='v2.0.0-rc1-42-gabc123'):
            self.assertEqual(package_build.git_dev_version(), 'v2.0.0-dev42')
            self.assertEqual(package_build.git_commit_hash(), 'abc123')


if __name__ == '__main__':
    unittest.main()
