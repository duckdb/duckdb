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

from scripts import exported_symbols_check


class ExportedSymbolsCheckTest(unittest.TestCase):
    def test_get_symbols_uses_nm_filter(self):
        result = subprocess.CompletedProcess([], 0, stdout="duckdb_api\n\nother_symbol\n", stderr="")
        with mock.patch.object(exported_symbols_check.subprocess, "run", return_value=result) as run:
            symbols = exported_symbols_check.get_symbols("libduckdb.so", "--defined-only")

        self.assertEqual(symbols, ["duckdb_api", "other_symbol"])
        run.assert_called_once_with(
            [
                "nm",
                "--extern-only",
                "--demangle",
                "--format=just-symbols",
                "--defined-only",
                "libduckdb.so",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_get_symbols_propagates_nm_failure(self):
        error = subprocess.CalledProcessError(1, ["nm"])
        with mock.patch.object(exported_symbols_check.subprocess, "run", side_effect=error):
            with self.assertRaises(subprocess.CalledProcessError):
                exported_symbols_check.get_symbols("libduckdb.so", "--defined-only")

    def test_find_culprits(self):
        defined_symbols = [
            "duckdb::Allowed()",
            "std::vector<int>::size()",
            "leaked_symbol",
            "std::random_device::operator()()",
        ]
        undefined_symbols = [
            "U",
            "puts",
            "std::random_device::random_device()",
        ]

        self.assertEqual(
            exported_symbols_check.find_culprits(defined_symbols, undefined_symbols),
            [
                "leaked_symbol",
                "std::random_device::operator()()",
                "std::random_device::random_device()",
            ],
        )

    def test_main_selects_defined_and_undefined_symbols(self):
        with tempfile.NamedTemporaryFile() as library:
            with mock.patch.object(
                exported_symbols_check,
                "get_symbols",
                side_effect=[["duckdb_api"], ["puts"]],
            ) as get_symbols:
                result = exported_symbols_check.main(["exported_symbols_check.py", library.name])

        self.assertEqual(result, 0)
        self.assertEqual(
            get_symbols.call_args_list,
            [
                mock.call(library.name, "--defined-only"),
                mock.call(library.name, "--undefined-only"),
            ],
        )


if __name__ == '__main__':
    unittest.main()
