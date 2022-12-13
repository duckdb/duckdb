import os
import sys
import sys

# path to target
basedir = os.getcwd()
target_dir = os.path.join(basedir, 'src', 'duckdb')
gyp_in = os.path.join(basedir, 'binding.gyp.in')
gyp_out = os.path.join(basedir, 'binding.gyp')

# path to package_build.py
os.chdir(os.path.join('..', '..'))
scripts_dir = 'scripts'
# list of extensions to bundle
extensions = ['parquet']

sys.path.append(scripts_dir)
import package_build

(source_list, include_list, original_sources) = package_build.build_package(target_dir, extensions, False)

# # the list of all source files (.cpp files) that have been copied into the `duckdb_source_copy` directory
# print(source_list)
# # the list of all include files
# print(include_list)

with open(gyp_in, 'r') as f:
	text = f.read()

source_list = [os.path.relpath(x, basedir) if os.path.isabs(x) else os.path.join('src', x) for x in source_list]
include_list = [os.path.join('src', 'duckdb', x) for x in include_list]

text = text.replace('${SOURCE_FILES}', ',\n                '.join(['"' + x + '"' for x in source_list]))
text = text.replace('${INCLUDE_FILES}', ',\n                '.join(['"' + x + '"' for x in include_list]))

with open(gyp_out, 'w+') as f:
	f.write(text)