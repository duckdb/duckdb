import os
import sys
import amalgamation
import pickle
import subprocess

# where to cache which files have already been compiled
cache_file = 'amalgamation.cache'
ignored_files = ['utf8proc_data.cpp']

RESUME_AUTO = 0
RESUME_ALWAYS = 1
RESUME_NEVER = 2

# resume behavior
# by default, we resume if the previous test_compile was run on the same commit hash as this one
resume = RESUME_AUTO
for arg in sys.argv:
	if arg == '--resume':
		resume = RESUME_ALWAYS
	elif arg == '--restart':
		cache = RESUME_NEVER

if resume == RESUME_NEVER:
	try:
		os.remove(cache_file)
	except:
		pass

def get_git_hash():
	proc = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE)
	return proc.stdout.read().strip()

current_hash = get_git_hash()

# load the cache, and check the commit hash
try:
	with open(cache_file, 'rb') as cf:
		cache = pickle.load(cf)
	if resume == RESUME_AUTO:
		# auto resume, check
		if cache['commit_hash'] != current_hash:
			cache = {}
except:
	cache = {}

cache['commit_hash'] = current_hash

def try_compilation(fpath, cache):
	if fpath in cache:
		return
	print(fpath)

	cmd = 'clang++ -std=c++11 -Wno-deprecated -Wno-writable-strings -S -MMD -MF dependencies.d -o deps.s ' + fpath + ' ' + ' '.join(["-I" + x for x in amalgamation.include_paths])
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
		if fname in amalgamation.excluded_compilation_files or fname in ignored_files:
			continue
		fpath = os.path.join(dir, fname)
		if os.path.isdir(fpath):
			compile_dir(fpath, cache)
		elif fname.endswith('.cpp') or fname.endswith('.hpp') or fname.endswith('.c') or fname.endswith('.cc'):
			try_compilation(fpath, cache)

# compile all files in the src directory (including headers!) individually
for cdir in amalgamation.compile_directories:
	compile_dir(cdir, cache)

