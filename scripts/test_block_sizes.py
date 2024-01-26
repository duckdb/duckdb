import os
import re
from python_helpers import open_utf8


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


current_dir = os.getcwd()
build_dir = os.path.join(os.getcwd(), 'build', 'release')

# run the fast tests and all storage-related tests
# with a block size of 16KB and a standard vector size
block_size = 16384
print("TESTING BLOCK_ALLOC_SIZE=%d" % (block_size,))
print("TESTING STANDARD_VECTOR_SIZE")

replace_in_file(
    'src/include/duckdb/storage/storage_info.hpp',
    r'constexpr static idx_t BLOCK_ALLOC_SIZE = \w+',
    'constexpr static idx_t BLOCK_ALLOC_SIZE = %d' % (block_size,),
)

execute_system_command('rm -rf build')
execute_system_command('make relassert')
execute_system_command('build/relassert/test/unittest')
execute_system_command('build/relassert/test/unittest "test/sql/storage/*"')

# run the fast tests and all storage-related tests
# with a block size of 16KB and a vector size of 512
vector_size = 512
print("TESTING BLOCK_ALLOC_SIZE=%d" % (block_size,))
print("TESTING STANDARD_VECTOR_SIZE=%d" % (vector_size,))

replace_in_file(
    'src/include/duckdb/common/vector_size.hpp',
    r'#define STANDARD_VECTOR_SIZE \w+',
    '#define STANDARD_VECTOR_SIZE %d' % (vector_size,),
)

execute_system_command('rm -rf build')
execute_system_command('make relassert')
execute_system_command('build/relassert/test/unittest')
execute_system_command('build/relassert/test/unittest "test/sql/storage/*"')
