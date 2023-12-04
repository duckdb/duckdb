import os, sys, re
from python_helpers import open_utf8

vector_sizes = [2]

current_dir = os.getcwd()
build_dir = os.path.join(os.getcwd(), 'build', 'release')


def execute_system_command(cmd):
    print(cmd)
    retcode = os.system(cmd)
    print(retcode)
    if retcode != 0:
        raise Exception


def replace_in_file(fname, regex, replace):
    with open_utf8(fname, 'r') as f:
        contents = f.read()
    contents = re.sub(regex, replace, contents)
    with open_utf8(fname, 'w+') as f:
        f.write(contents)


for vector_size in vector_sizes:
    print("TESTING STANDARD_VECTOR_SIZE=%d" % (vector_size,))
    replace_in_file(
        'src/include/duckdb/common/vector_size.hpp',
        r'#define STANDARD_VECTOR_SIZE \d+',
        '#define STANDARD_VECTOR_SIZE %d' % (vector_size,),
    )
    execute_system_command('rm -rf build')
    execute_system_command('make relassert')
    execute_system_command('python3 scripts/run_tests_one_by_one.py build/relassert/test/unittest')
