import os
import subprocess

version = '0.53.1'
max_size_growth = 1.1


if not os.path.exists(f'v{version}.tar.gz'):
    assert(os.system(f'wget https://github.com/yhirose/cpp-httplib/archive/refs/tags/v{version}.tar.gz') == 0)
assert(os.system(f'tar xvf v{version}.tar.gz --strip-components 1 cpp-httplib-{version}/httplib.h') == 0)
assert(os.system(f'mv httplib.h httplib.hpp') == 0)

delimiter = r"([^[:alnum:]_]|$)"
rules = [
    (r"std::unique_lock" + delimiter, r"duckdb::unique_lock\1"),
    (r"std::regex_match" + delimiter, r"RegexMatch\1"),
    (r"std::regex" + delimiter, r"Regex\1"),
    (r"std::regex_search" + delimiter, r"duckdb_re2::RegexSearch\1"),
    (r"std::make_shared" + delimiter, r"duckdb::make_shared_ptr\1"),
    (r"namespace httplib" + delimiter, r"namespace CPPHTTPLIB_NAMESPACE\1"),
    (r"httplib::", r"CPPHTTPLIB_NAMESPACE::"),
    (r"std::smatch" + delimiter, r"duckdb_re2::Match\1"),
    (r"std::regex_constants::icase" + delimiter, r"duckdb_re2::RegexOptions::CASE_INSENSITIVE\1"),
    (r"std::stringstream" + delimiter, r"duckdb::stringstream\1"),
    (r"std::unique_ptr" + delimiter, r"duckdb::unique_ptr\1"),
    (r"(^|[^:])make_matcher" + delimiter, r"\1Regex\2"),
    (r"thread_local" + delimiter, r"REGEX_SCOPE\1"),
]


def apply_rules(file: str):
    original_size = os.path.getsize(file)
    for (target, replacement) in rules:
        print(f"Applying rule: {target} -> {replacement}")
        subprocess.run(["sed", "-E", "-i", "", f"s#{target}#{replacement}#g", file], check=True)
    transformed_size = os.path.getsize(file)
    assert transformed_size <= original_size * max_size_growth, \
        f"{file} grew from {original_size} to {transformed_size} bytes"


apply_rules("httplib.hpp")