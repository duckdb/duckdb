# these are the remnants of the once-proud amalgamation.py
# we still use those in some places, e.g. test_compile.py

import package_build
import os

src_dir = 'src'
compile_directories = [src_dir] + package_build.third_party_sources() + ['extension/loader']
include_dir = os.path.join(src_dir, 'include')
include_paths = [include_dir] + package_build.third_party_includes()
excluded_files = ['grammar.cpp', 'grammar.hpp', 'symbols.cpp']
# files excluded from individual file compilation during test_compile
excluded_compilation_files = excluded_files + ['gram.hpp', 'kwlist.hpp', "duckdb-c.cpp"]
