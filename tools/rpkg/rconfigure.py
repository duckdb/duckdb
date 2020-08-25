import os
import sys
import shutil
import subprocess

if not os.path.isfile(os.path.join('..', '..', 'scripts', 'amalgamation.py')):
	print("Could not find amalgamation script! This script needs to be launched from the subdirectory tools/rpkg")
	exit(1)

target_dir = os.path.join(os.getcwd(), 'src', 'duckdb')

if not os.path.isdir(target_dir):
	os.mkdir(target_dir)

prev_wd = os.getcwd()
os.chdir(os.path.join('..', '..'))

# read the source id
proc = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
githash = proc.stdout.read().strip()


# obtain the list of source files from the amalgamation
sys.path.append('scripts')
import amalgamation
source_list = amalgamation.list_sources()
include_list = amalgamation.list_include_dirs()
include_files = amalgamation.list_includes()

def copy_file(src, target_dir):
	# get the path
	full_path = src.split(os.path.sep)
	current_path = target_dir
	for i in range(len(full_path) - 1):
		current_path = os.path.join(current_path, full_path[i])
		if not os.path.isdir(current_path):
			os.mkdir(current_path)
	amalgamation.copy_if_different(src, os.path.join(current_path, full_path[-1]))


# now do the same for the parquet extension
sys.path.append(os.path.join('extension', 'parquet'))
import parquet_amalgamation
parquet_include_directories = parquet_amalgamation.include_directories

include_files += amalgamation.list_includes_files(parquet_include_directories)

include_list += parquet_include_directories
source_list += parquet_amalgamation.source_files

for src in source_list:
	copy_file(src, target_dir)

for inc in include_files:
	copy_file(inc, target_dir)

# object list
object_list = ' '.join([os.path.join('duckdb', x.rsplit('.', 1)[0] + '.o') for x in source_list])
# include list
include_list = ' '.join(['-I' + os.path.join('duckdb', x) for x in include_list])
# add source id define
include_list += ' -DDUCKDB_SOURCE_ID=\\"{}\\"'.format(githash)

os.chdir(prev_wd)

# read Makevars.in and replace the {{ SOURCES }} and {{ INCLUDES }} macros
with open(os.path.join('src', 'Makevars.in'), 'r') as f:
	text = f.read()

text = text.replace('{{ SOURCES }}', object_list)
text = text.replace('{{ INCLUDES }}', include_list)

# now write it to the output Makevars
with open(os.path.join('src', 'Makevars'), 'w+') as f:
	f.write(text)
