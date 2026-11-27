#!/usr/bin/env python3
import shutil
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CXX = shutil.which("clang++") or shutil.which("g++")
DIAGNOSTIC = "Match std::exception with ExceptionMatches from test_exception_matcher.hpp for DuckDB exceptions"
MACROS = [
    "REQUIRE_THROWS_AS",
    "CHECK_THROWS_AS",
    "CATCH_REQUIRE_THROWS_AS",
    "CATCH_CHECK_THROWS_AS",
    "REQUIRE_THROWS_MATCHES",
    "CHECK_THROWS_MATCHES",
    "CATCH_REQUIRE_THROWS_MATCHES",
    "CATCH_CHECK_THROWS_MATCHES",
]
PREAMBLE = """
#include "catch.hpp"
#include "test_exception_matcher.hpp"
#include <new>

using ExceptionAlias = duckdb::IOException;
using StandardExceptionAlias = std::runtime_error;
class DerivedException : public duckdb::IOException {
public:
    using duckdb::IOException::IOException;
};
"""


@unittest.skipUnless(CXX, "requires clang++ or g++")
class CatchExceptionAssertionsTest(unittest.TestCase):
    def compile_source(self, source, disable_rtti, prefix_all=False, disable_assertions=False):
        command = [
            CXX,
            "-std=c++17",
            f"-I{REPO_ROOT / 'test/include'}",
            f"-I{REPO_ROOT / 'src/include'}",
            "-fsyntax-only",
            "-x",
            "c++",
            "-",
        ]
        if disable_rtti:
            command.insert(1, "-fno-rtti")
        if prefix_all:
            command.insert(1, "-DCATCH_CONFIG_PREFIX_ALL")
        if disable_assertions:
            command.insert(1, "-DCATCH_CONFIG_DISABLE")
        return subprocess.run(command, input=source, text=True, capture_output=True, timeout=60)

    def assertion(self, macro, exception_type):
        args = f'throw {exception_type}("example"), {exception_type}'
        if macro.endswith("_MATCHES"):
            args += f", Catch::Predicate<{exception_type}>([](const {exception_type} &) {{ return true; }})"
        return f"{macro}({args});"

    def test_duckdb_exception_types_are_rejected(self):
        for disable_rtti in (False, True):
            for index, macro in enumerate(MACROS):
                exception_type = ("duckdb::IOException", "ExceptionAlias", "DerivedException")[index % 3]
                with self.subTest(macro=macro, exception_type=exception_type, disable_rtti=disable_rtti):
                    source = PREAMBLE + "void TestAssertions() { " + self.assertion(macro, exception_type) + " }\n"
                    result = self.compile_source(source, disable_rtti, prefix_all=macro.startswith("CATCH_"))
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(DIAGNOSTIC, result.stderr)

    def test_standard_exceptions_and_message_assertions_are_accepted(self):
        for disable_rtti in (False, True):
            for prefix_all in (False, True):
                prefix = "CATCH_" if prefix_all else ""
                assertions = [
                    self.assertion(macro, "std::runtime_error")
                    for macro in MACROS
                    if macro.startswith("CATCH_") == prefix_all
                ]
                assertions.extend(
                    [
                        f"{prefix}REQUIRE_THROWS_AS(throw std::bad_alloc(), std::bad_alloc);",
                        f'{prefix}REQUIRE_THROWS_AS(throw StandardExceptionAlias("example"), StandardExceptionAlias);',
                        f'{prefix}REQUIRE_THROWS_WITH(throw duckdb::IOException("example"), Catch::Contains("example"));',
                        f'{prefix}CHECK_THROWS_WITH(throw ExceptionAlias("example"), Catch::Contains("example"));',
                        f'{prefix}REQUIRE_THROWS_MATCHES(throw duckdb::IOException("example"), std::exception, '
                        'duckdb::ExceptionMatches(duckdb::ExceptionType::IO, Catch::Contains("example")));',
                    ]
                )
                source = PREAMBLE + "void TestAssertions() {\n" + "\n".join(assertions) + "\n}\n"
                for disable_assertions in (False, True):
                    with self.subTest(
                        disable_rtti=disable_rtti, prefix_all=prefix_all, disable_assertions=disable_assertions
                    ):
                        result = self.compile_source(source, disable_rtti, prefix_all, disable_assertions)
                        self.assertEqual(result.returncode, 0, result.stderr)

    def test_disabled_assertions_still_reject_duckdb_exception_types(self):
        source = (
            PREAMBLE
            + "void TestAssertions() { REQUIRE_THROWS_AS(throw ExceptionAlias(\"example\"), ExceptionAlias); }\n"
        )
        result = self.compile_source(source, disable_rtti=True, disable_assertions=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(DIAGNOSTIC, result.stderr)


if __name__ == "__main__":
    unittest.main()
