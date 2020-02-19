import os, sys
vector_sizes = [2, 4, 8, 16, 32, 64, 128, 256, 512]

current_dir = os.getcwd()
build_dir = os.path.join(os.getcwd(), 'build', 'release')

def execute_system_command(cmd):
	if os.system != 0:
		raise Exception

for vector_size in vector_sizes:
	print("TESTING STANDARD_VECTOR_SIZE=%d" % (vector_size,))
	execute_system_command('rm -rf build')
	execute_system_command('mkdir -p build/release')
	os.chdir(build_dir)
	execute_system_command('cmake -DCMAKE_BUILD_TYPE=Release -DSTANDARD_VECTOR_SIZE=%d ../..')
	execute_system_command('cmake --build .')
	os.chdir(current_dir)
	execute_system_command('build/release/test/unittest "*"')
