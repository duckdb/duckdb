import os

vector_sizes = [2]

current_dir = os.getcwd()
build_dir = os.path.join(os.getcwd(), 'build', 'release')


def execute_system_command(cmd):
    print(cmd)
    retcode = os.system(cmd)
    print(retcode)
    if retcode != 0:
        raise Exception(f"Failed to run command {cmd} - exit code {retcode}")


for vector_size in vector_sizes:
    print("TESTING STANDARD_VECTOR_SIZE=%d" % (vector_size,))
    execute_system_command('rm -rf build')
    execute_system_command(f'STANDARD_VECTOR_SIZE={vector_size} make relassert')
    execute_system_command('python3 scripts/run_tests_one_by_one.py build/relassert/test/unittest --no-exit')
