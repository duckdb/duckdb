# this script creates a single header + source file combination out of the DuckDB sources
header_file = "duckdb.hpp"
source_file = "duckdb.cpp"
cache_file = 'amalgamation.cache'
include_paths = ["src/include", "third_party/hyperloglog", "third_party/re2", "third_party/miniz", "third_party/libpg_query/include", "third_party/libpg_query"]
compile_directories = ['third_party/hyperloglog', 'third_party/libpg_query', 'third_party/miniz', 'third_party/re2', 'src']
excluded_files = ["duckdb-c.cpp", "scan.c"]


pg_substitutions = {
	"AlterTableType": "PGAlterTableType",
	"Constraint": "PGConstraint",
	"Date": "PGDate",
	"Expr": "PGExpr",
	"Index": "PGIndex",
	"Interval": "PGInterval",
	"JoinType": "PGJoinType",
	"Node": "PGNode",
	"List": "PGList",
	"Timestamp": "PGTimestamp",
	"Undefined": "PGUndefined",
	"Oid": "PGOid",
	"Value": "PGValue"
}

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

def get_includes(fpath):
	with open(fpath, 'r') as f:
		text = f.read()
	# find all the includes referred to in the directory
	include_statements = re.findall("^[#]include [\"]([^\"]+)", text, flags=re.MULTILINE)
	include_files = []
	# figure out where they are located
	for statement in include_statements:
		found = False
		for include_path in include_paths:
			ipath = os.path.join(include_path, statement)
			if os.path.isfile(ipath):
				include_files.append(ipath)
				found = True
				break
		if not found:
			raise Exception('Could not find include file "' + statement + '", included from file "' + fpath + '"')
	return include_files

def cleanup_file(fpath, text):
	if fpath.startswith("third_party/libpg_query"):
		for sub in pg_substitutions.keys():
			text = re.sub("([ \t\n*,(])" + sub + "([ \t\n*,;])", "\g<1>" + pg_substitutions[sub] + "\g<2>", text)
	else:
		for sub in pg_substitutions.keys():
			text = re.sub("postgres::" + sub + "([ \t\n*,;])", pg_substitutions[sub] + "\g<1>", text)
		text = re.sub("postgres::", "", text)
		text = re.sub("PostgresParser", "postgres::PostgresParser", text)


	# remove all includes of non-system headers
	text = re.sub("^[#]include [\"][^\n]+", "", text, flags=re.MULTILINE)
	# remove all "#pragma once" notifications
	text = re.sub('#pragma once', '', text)
	return text

# recursively get all includes and write them
written_headers = {}

def write_file(current_file, hfile, write_line_pragma=False):
	if current_file.split('/')[-1] in excluded_files:
		print(current_file)
		return
	if current_file in written_headers:
		# header is already written
		return
	written_headers[current_file] = True

	print(current_file)

	# find includes of this header
	includes = get_includes(current_file)
	# now write all the dependencies of this header first
	for include in includes:
		write_file(include, hfile, write_line_pragma)
	# now read the header and write it
	with open(current_file, 'r') as f:
		if write_line_pragma:
			hfile.write('#line 0 "' + current_file + '"\n')
		hfile.write(cleanup_file(current_file, f.read()))

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
	write_file('src/include/duckdb.hpp', hfile)

def write_dir(dir, sfile):
	files = os.listdir(dir)
	for fname in files:
		fpath = os.path.join(dir, fname)
		if os.path.isdir(fpath):
			write_dir(fpath, sfile)
		elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
			write_file(fpath, sfile, True)

# now construct duckdb.cpp
print("------------------------")
print("-- Writing duckdb.cpp --")
print("------------------------")

# scan all the .cpp files
with open(source_file, 'w+') as sfile:
	sfile.write('#include "duckdb.hpp"\n\n')
	for compile_dir in compile_directories:
		write_dir(compile_dir, sfile)
