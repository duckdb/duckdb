import os, sys, re
vector_sizes = [2, 512]

current_dir = os.getcwd()
build_dir = os.path.join(os.getcwd(), 'build', 'release')

def execute_system_command(cmd):
	print(cmd)
	retcode = os.system(cmd)
	print(retcode)
	if retcode != 0:
		raise Exception

def replace_in_file(fname, regex, replace):
	with open(fname, 'r') as f:
		contents = f.read()
	contents = re.sub(regex, replace, contents)
	with open(fname, 'w+') as f:
		f.write(contents)

for vector_size in vector_sizes:
	print("TESTING STANDARD_VECTOR_SIZE=%d" % (vector_size,))
	replace_in_file('src/include/duckdb/common/constants.hpp', '#define STANDARD_VECTOR_SIZE \d+', '#define STANDARD_VECTOR_SIZE %d' % (vector_size,))
	execute_system_command('rm -rf build')
	execute_system_command('mkdir -p build/release')
	os.chdir(build_dir)
	execute_system_command('cmake -DCMAKE_BUILD_TYPE=Release ../..')
	execute_system_command('cmake --build .')
	os.chdir(current_dir)
	execute_system_command('build/release/test/unittest')
