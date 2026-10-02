#!/usr/bin/env python3

import contextlib
import io
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


class ExtensionSyncTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def parse(self, content):
        config = self.root / 'extension_config.cmake'
        config.write_text(content)
        return sync.parse_cmake_file(config)

    def test_quoted_submodule_and_git_arguments(self):
        extensions = self.parse(
            '''
            duckdb_extension_load(avro
                GIT_URL "https://example.com/avro"
                GIT_TAG "012345"
                SUBMODULES "third_party/avro-c"
                LOAD_TESTS APPLY_PATCHES
            )
            '''
        )
        self.assertEqual(
            extensions,
            [
                {
                    'name': 'avro',
                    'git_url': 'https://example.com/avro',
                    'git_tag': '012345',
                    'submodules': ['third_party/avro-c'],
                    'apply_patches': True,
                }
            ],
        )

    def test_comments_and_quoted_command_text_are_ignored(self):
        extensions = self.parse(
            '''
            # duckdb_extension_load(commented GIT_URL unused GIT_TAG unused)
            #[=[
            duckdb_extension_load(block_comment GIT_URL unused GIT_TAG unused)
            ]=]
            message("duckdb_extension_load(quoted GIT_URL unused GIT_TAG unused)")
            set(example [[duckdb_extension_load(bracket GIT_URL unused GIT_TAG unused)]])
            duckdb_extension_load(active
                GIT_URL "https://example.com/repo#fragment"
                GIT_TAG main # SUBMODULES ignored APPLY_PATCHES
            )
            '''
        )
        self.assertEqual([ext['name'] for ext in extensions], ['active'])
        self.assertEqual(extensions[0]['git_url'], 'https://example.com/repo#fragment')
        self.assertEqual(extensions[0]['submodules'], [])
        self.assertFalse(extensions[0]['apply_patches'])

    def test_submodule_lists_spaces_and_parentheses(self):
        extensions = self.parse(
            r'''
            duckdb_extension_load(avro GIT_URL url GIT_TAG revision
                SUBMODULES "third_party/avro-c;path with spaces" "path(with)parens" escaped\ path
                DONT_LINK LOAD_TESTS)
            '''
        )
        self.assertEqual(
            extensions[0]['submodules'],
            ['third_party/avro-c', 'path with spaces', 'path(with)parens', 'escaped path'],
        )

    def test_empty_and_bracket_submodules(self):
        extensions = self.parse(
            '''
            duckdb_extension_load(empty GIT_URL url GIT_TAG revision SUBMODULES "")
            duckdb_extension_load(bracket GIT_URL url GIT_TAG revision SUBMODULES [=[path with spaces]=])
            '''
        )
        self.assertEqual(extensions[0]['submodules'], [])
        self.assertEqual(extensions[1]['submodules'], ['path with spaces'])

    def test_includes_respect_comments_and_declaration_order(self):
        included_dir = self.root / 'extensions'
        included_dir.mkdir()
        (included_dir / 'active.cmake').write_text('duckdb_extension_load(active GIT_URL url GIT_TAG revision)')
        (included_dir / 'ignored.cmake').write_text('duckdb_extension_load(ignored GIT_URL url GIT_TAG revision)')
        extensions = self.parse(
            '''
            duckdb_extension_load(first GIT_URL url GIT_TAG revision)
            # include("${EXTENSION_CONFIG_BASE_DIR}/ignored.cmake")
            include("${EXTENSION_CONFIG_BASE_DIR}/active.cmake")
            '''
        )
        self.assertEqual([ext['name'] for ext in extensions], ['first', 'active'])

    def test_in_tree_extensions_are_ignored(self):
        self.assertEqual(
            self.parse('duckdb_extension_load(json)\nduckdb_extension_load(local SOURCE_DIR /tmp/local)'), []
        )

    def make_named_configs(self):
        config_dir = self.root / '.github' / 'config' / 'extensions'
        config_dir.mkdir(parents=True)
        for name in ['httpfs', 'avro', 'aws']:
            (config_dir / f'{name}.cmake').write_text(
                f'duckdb_extension_load({name} GIT_URL https://example.com/{name} GIT_TAG revision)'
            )

    def test_core_extensions_are_additive_and_deduplicated(self):
        self.make_named_configs()
        os.environ.update(BUILD_EXTENSIONS='avro;httpfs', CORE_EXTENSIONS="'httpfs;parquet;tpch'")
        self.assertEqual(list(sync.collect_extensions(self.root)), ['avro', 'httpfs'])

    def test_core_extensions_without_build_extensions(self):
        self.make_named_configs()
        os.environ['CORE_EXTENSIONS'] = "'httpfs;parquet;tpch'"
        self.assertEqual(list(sync.collect_extensions(self.root)), ['httpfs'])

    def test_make_combines_quoted_core_extensions_with_build_extensions(self):
        self.make_named_configs()
        self.assertEqual(
            list(sync.collect_extensions(self.root, "avro;'httpfs;parquet;tpch'")),
            ['avro', 'httpfs'],
        )

    def test_explicit_build_extensions_override_aliases(self):
        self.make_named_configs()
        os.environ.update(BUILD_EXTENSIONS='aws', DUCKDB_EXTENSIONS='aws', CORE_EXTENSIONS='httpfs')
        self.assertEqual(list(sync.collect_extensions(self.root, 'avro')), ['avro', 'httpfs'])

    def test_duckdb_extensions_alias(self):
        self.make_named_configs()
        os.environ.update(DUCKDB_EXTENSIONS='aws', CORE_EXTENSIONS='httpfs')
        self.assertEqual(list(sync.collect_extensions(self.root)), ['aws', 'httpfs'])

    def test_sync_clones_only_required_submodule_and_reuses_checkout(self):
        os.environ.update(
            PATH=os.defpath,
            GIT_ALLOW_PROTOCOL='file',
            GIT_CONFIG_NOSYSTEM='1',
            GIT_CONFIG_GLOBAL=os.devnull,
            GIT_AUTHOR_NAME='Extension Sync Test',
            GIT_AUTHOR_EMAIL='test@example.com',
            GIT_COMMITTER_NAME='Extension Sync Test',
            GIT_COMMITTER_EMAIL='test@example.com',
        )

        def git(repo, *args):
            return subprocess.check_output(['git', '-C', str(repo), *args], stderr=subprocess.PIPE, text=True).strip()

        def repo(name):
            directory = self.root / name
            directory.mkdir()
            git(directory, 'init')
            (directory / 'source.txt').write_text(name)
            git(directory, 'add', '.')
            git(directory, 'commit', '-m', 'Initial source')
            return directory

        docs = repo('docs')
        library = repo('library')
        git(library, 'submodule', 'add', str(docs), 'doc/theme')
        git(library, 'commit', '-am', 'Add documentation theme')
        extension = repo('extension')
        git(extension, 'submodule', 'add', str(library), 'third_party/library')
        git(extension, 'commit', '-am', 'Add library')
        revision = git(extension, 'rev-parse', 'HEAD')
        descriptor = self.parse(
            f'duckdb_extension_load(example GIT_URL "{extension}" GIT_TAG "{revision}" SUBMODULES "third_party/library")'
        )[0]
        external_dir = self.root / 'external'
        with contextlib.redirect_stdout(io.StringIO()):
            sync.sync_extension(descriptor, external_dir, self.root)
        checkout = external_dir / 'example'
        self.assertEqual((checkout / 'third_party/library/source.txt').read_text(), 'library')
        self.assertFalse((checkout / 'third_party/library/doc/theme/source.txt').exists())

        with patch.object(sync, 'run_cmd', wraps=sync.run_cmd) as commands, contextlib.redirect_stdout(io.StringIO()):
            sync.sync_extension(descriptor, external_dir, self.root)
        self.assertFalse(
            any(call.args[0][1] in ('clone', 'fetch', 'checkout', 'submodule') for call in commands.call_args_list)
        )
        self.assertEqual(git(checkout, 'rev-parse', 'HEAD'), revision)


if __name__ == '__main__':
    unittest.main()
