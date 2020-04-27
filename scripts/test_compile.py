import os, sys, amalgamation, pickle

# where to cache which files have already been compiled, only used for --compile --resume
cache_file = 'amalgamation.cache'

resume = False

for arg in sys.argv:
	if arg == '--resume':
		resume = True

if not resume:
	try:
		os.remove(cache_file)
	except:
		pass

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
		if fname in amalgamation.excluded_compilation_files:
			continue
		fpath = os.path.join(dir, fname)
		if os.path.isdir(fpath):
			compile_dir(fpath, cache)
		elif fname.endswith('.cpp') or fname.endswith('.hpp') or fname.endswith('.c') or fname.endswith('.cc'):
			try_compilation(fpath, cache)

# compilation pass only
# compile all files in the src directory (including headers!) individually
try:
	with open(cache_file, 'rb') as cf:
		cache = pickle.load(cf)
except:
	cache = {}

for cdir in amalgamation.compile_directories:
	compile_dir(cdir, cache)

