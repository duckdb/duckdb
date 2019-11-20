# this script creates a single header + source file combination out of the DuckDB sources
header_file = "duckdb.hpp"
source_file = "duckdb.cpp"
cache_file = 'amalgamation.cache'
include_paths = ["src/include", "third_party/hyperloglog", "third_party/re2", "third_party/miniz", "third_party/libpg_query/include", "third_party/libpg_query"]
compile_directories = ['src', 'third_party/hyperloglog', 'third_party/miniz', 'third_party/re2', 'third_party/libpg_query']
excluded_files = ["duckdb-c.cpp", 'grammar.cpp', 'grammar.hpp', 'gram.hpp', 'kwlist.hpp', 'symbols.cpp']

import os, re, sys, pickle

compile = False
resume = False

for arg in sys.argv:
	if arg == '--compile':
		compile = True
	elif arg == '--resume':
		resume = True

if not resume:
	try:
		os.remove(cache_file)
	except:
		pass

def get_includes(fpath, text):
	# find all the includes referred to in the directory
	include_statements = re.findall("(^[#]include[\t ]+[\"]([^\"]+)[\"])", text, flags=re.MULTILINE)
	include_files = []
	# figure out where they are located
	for statement in [x[1] for x in include_statements]:
		found = False
		for include_path in include_paths:
			ipath = os.path.join(include_path, statement)
			if os.path.isfile(ipath):
				include_files.append(ipath)
				found = True
				break
		if not found:
			raise Exception('Could not find include file "' + statement + '", included from file "' + fpath + '"')
	return ([x[0] for x in include_statements], include_files)

def cleanup_file(text):
	# remove all "#pragma once" notifications
	text = re.sub('#pragma once', '', text)
	return text

# recursively get all includes and write them
written_files = {}

def write_file(current_file):
	global written_files
	if current_file.split('/')[-1] in excluded_files:
		# file is in ignored files set
		return ""
	if current_file in written_files:
		# file is already written
		return ""
	written_files[current_file] = True

	# first read this file
	with open(current_file, 'r') as f:
		text = f.read()

	(statements, includes) = get_includes(current_file, text)
	# now write all the dependencies of this header first
	for i in range(len(includes)):
		include_text = write_file(includes[i])
		text = text.replace(statements[i], include_text)

	print(current_file)
	# now read the header and write it
	return cleanup_file(text)

def try_compilation(fpath, cache):
	if fpath in cache:
		return
	print(fpath)

	cmd = 'clang++ -std=c++11 -Wno-deprecated -Wno-writable-strings -S -MMD -MF dependencies.d -o deps.s ' + fpath + ' ' + ' '.join(["-I" + x for x in include_paths])
	ret = os.system(cmd)
	if ret != 0:
		raise Exception('Failed compilation of file "' + fpath + '"!\n Command: ' + cmd)
	cache[fpath] = True
	with open(cache_file, 'wb') as cf:
		pickle.dump(cache, cf)

def compile_dir(dir, cache):
	files = os.listdir(dir)
	files.sort()
	for fname in files:
		if fname in excluded_files:
			continue
		fpath = os.path.join(dir, fname)
		if os.path.isdir(fpath):
			compile_dir(fpath, cache)
		elif fname.endswith('.cpp') or fname.endswith('.hpp') or fname.endswith('.c') or fname.endswith('.cc'):
			try_compilation(fpath, cache)

if compile:
	# compilation pass only
	# compile all files in the src directory (including headers!) individually
	try:
		with open(cache_file, 'rb') as cf:
			cache = pickle.load(cf)
	except:
		cache = {}
	for cdir in compile_directories:
		compile_dir(cdir, cache)
	exit(0)

# now construct duckdb.hpp from these headers
print("-----------------------")
print("-- Writing duckdb.hpp --")
print("-----------------------")
with open(header_file, 'w+') as hfile:
	hfile.write("#pragma once\n")
	hfile.write(write_file('src/include/duckdb.hpp'))

def write_dir(dir, sfile):
	files = os.listdir(dir)
	files.sort()
	for fname in files:
		if fname in excluded_files:
			continue
		fpath = os.path.join(dir, fname)
		if os.path.isdir(fpath):
			write_dir(fpath, sfile)
		elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
			sfile.write(write_file(fpath))

# now construct duckdb.cpp
print("------------------------")
print("-- Writing duckdb.cpp --")
print("------------------------")

# scan all the .cpp files
with open(source_file, 'w+') as sfile:
	sfile.write('#include "duckdb.hpp"\n\n')
	for compile_dir in compile_directories:
		write_dir(compile_dir, sfile)
