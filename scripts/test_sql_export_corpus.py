"""Tests for deterministic SQL export corpus discovery and freezing."""

from pathlib import Path
import tempfile
import unittest

import sql_export_corpus as corpus


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.original_root = corpus.ROOT
        corpus.ROOT = Path(self.temp.name)
        self.manifest = corpus.ROOT / 'manifest.txt'
        for name in ['a.test', 'nested/b.test', 'literal[1].test', 'ignored.test_slow']:
            path = corpus.ROOT / 'test/sql' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

    def tearDown(self):
        corpus.ROOT = self.original_root
        self.temp.cleanup()

    def test_discovery_deduplicates_overlaps_and_applies_exclusions(self):
        self.manifest.write_text('# Public SQL tests\ntest/sql/**/*.test\ntest/sql/a.test\n!test/sql/nested/*.test\n')
        self.assertEqual(corpus.resolve_manifest(self.manifest), ['test/sql/a.test', 'test/sql/literal[1].test'])

    def test_literal_manifest_and_freeze_are_stable(self):
        self.manifest.write_text('test/sql/literal[1].test\ntest/sql/a.test\n')
        output = corpus.ROOT / 'resolved.txt'
        corpus.freeze_manifest(self.manifest, output)
        expected = 'test/sql/a.test\ntest/sql/literal[1].test\n'
        self.assertEqual(output.read_text(), expected)
        (corpus.ROOT / 'test/sql/new.test').touch()
        self.assertEqual(corpus.resolve_manifest(output), expected.splitlines())

    def test_new_tests_join_discovery(self):
        self.manifest.write_text('test/sql/**/*.test\n')
        before = corpus.resolve_manifest(self.manifest)
        (corpus.ROOT / 'test/sql/new.test').touch()
        self.assertEqual(corpus.resolve_manifest(self.manifest), sorted(before + ['test/sql/new.test']))

    def test_invalid_and_empty_selections_fail(self):
        for text in [
            '',
            'test/sql/no-match*.test',
            'test/sql/../../secret.test',
            '/tmp/private.test',
            'test/sql/ignored.test_slow',
            'test/sql/**/*.test\n!test/sql/**/*.test',
        ]:
            with self.subTest(text=text):
                self.manifest.write_text(text)
                with self.assertRaises(ValueError):
                    corpus.resolve_manifest(self.manifest)


if __name__ == '__main__':
    unittest.main()
