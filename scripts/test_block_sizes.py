import os


def execute_system_command(cmd):
    print(cmd)
    retcode = os.system(cmd)
    print(retcode)
    if retcode != 0:
        raise Exception


current_dir = os.getcwd()
build_dir = os.path.join(os.getcwd(), 'build', 'release')

# run the fast tests and all storage-related tests
# with a block size of 16KB and a standard vector size
block_size = 16384
print("TESTING BLOCK_ALLOC_SIZE=%d" % (block_size,))
print("TESTING STANDARD_VECTOR_SIZE")

execute_system_command('rm -rf build')
execute_system_command(f'BLOCK_ALLOC_SIZE={block_size} make relassert')
execute_system_command('build/relassert/test/unittest')
execute_system_command('build/relassert/test/unittest "test/sql/storage/*"')

# run the fast tests and all storage-related tests
# with a block size of 16KB and a vector size of 512
vector_size = 512
print("TESTING BLOCK_ALLOC_SIZE=%d" % (block_size,))
print("TESTING STANDARD_VECTOR_SIZE=%d" % (vector_size,))

execute_system_command('rm -rf build')
execute_system_command(f'BLOCK_ALLOC_SIZE={block_size} STANDARD_VECTOR_SIZE={vector_size} make release')
execute_system_command('build/release/test/unittest')
