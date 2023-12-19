import os, re
from python_helpers import open_utf8

# we want to test different block sizes with different vector sizes
block_sizes = [16384]
vector_sizes = [2048, 512]

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


for block_size in block_sizes:
    print("TESTING BLOCK_ALLOC_SIZE=%d" % (block_size,))
    replace_in_file(
        'src/include/duckdb/storage/storage_info.hpp',
        r'constexpr static idx_t BLOCK_ALLOC_SIZE = \d+',
        'constexpr static idx_t BLOCK_ALLOC_SIZE = %d' % (block_size,),
    )

    for vector_size in vector_sizes:
        print("TESTING STANDARD_VECTOR_SIZE=%d" % (vector_size,))
        replace_in_file(
            'src/include/duckdb/common/vector_size.hpp',
            r'#define STANDARD_VECTOR_SIZE \d+',
            '#define STANDARD_VECTOR_SIZE %d' % (vector_size,),
        )

        execute_system_command('rm -rf build')
        execute_system_command('make relassert')
        execute_system_command('python3 scripts/run_tests_one_by_one.py build/relassert/test/unittest "*"')
