import os
import sys
import shutil
from pathlib import Path
from string import Template

# list of extensions to bundle
extensions = ['parquet', 'icu', 'json']

# name of the repository
repo_name = 'duckdb-swift'

# name of the Swift package C target
swift_target_name = 'Cduckdb'

# name of the target's source dir
src_dir_name = 'duckdb'

# path to target
base_dir = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else os.getcwd())
package_dir = os.path.join(base_dir, repo_name)
target_dir = os.path.join(package_dir, 'Sources', swift_target_name)
includes_dir = os.path.join(target_dir, 'include')
src_dir = os.path.join(target_dir, src_dir_name)

# Prepare target directory
Path(target_dir).mkdir(parents=True, exist_ok=True)
Path(includes_dir).mkdir(parents=True, exist_ok=True)

# build package source files
os.chdir(base_dir)
os.chdir(os.path.join('..', '..'))
sys.path.append('scripts')
import package_build

# fresh build - copy over all of the files
(source_list, include_list, _) = package_build.build_package(src_dir, extensions, 32, src_dir_name)
# standardise paths
source_list = [os.path.relpath(x, target_dir) if os.path.isabs(x) else x for x in source_list]
include_list = [os.path.join(src_dir_name, x) for x in include_list]
define_list = ['DUCKDB_EXTENSION_{}_LINKED'.format(ext.upper()) for ext in extensions]
# write Package.swift
os.chdir(base_dir)

# copy umbrella header to path SPM expects (auto .modulemap)
header_file_src = os.path.join(src_dir, 'src', 'include', 'duckdb.h')
header_file_dest = os.path.join(includes_dir, 'duckdb.h')
shutil.copyfile(header_file_src, header_file_dest)

source_list_strs = ['"' + x + '",' for x in source_list]
include_list_strs = ['.headerSearchPath("' + x + '"),' for x in include_list]
define_list_strs = ['.define("' + x + '"),' for x in define_list]
src_line_prefix = '\n        '  # indents eight spaces

content = {
    'source_list': src_line_prefix.join(source_list_strs),
    'search_path_list': src_line_prefix.join(include_list_strs),
    'define_list': src_line_prefix.join(define_list_strs),
}

package_manifest_path = os.path.join(package_dir, 'Package.swift')
with open('Package.swift.template', 'r') as f:
    src = Template(f.read())
    result = src.substitute(content)
    with open(package_manifest_path, 'w') as f:
        f.write(result)
