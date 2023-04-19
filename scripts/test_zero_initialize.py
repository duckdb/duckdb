import os
import argparse
import subprocess
import shutil

parser = argparse.ArgumentParser(description='''Runs storage tests both with explicit one-initialization and with explicit zero-initialization, and verifies that the final storage files are the same.
The purpose of this is to verify all memory is correctly initialized before writing to disk - which prevents leaking of in-memory data in storage files by writing uninitialized memory to disk.''')
parser.add_argument('--unittest', default='build/debug/test/unittest', help='path to unittest', dest='unittest')
parser.add_argument('--zero_init_dir', default='test_zero_init_db', help='directory to write zero-initialized databases to', dest='zero_init_dir')
parser.add_argument('--standard_dir', default='test_standard_db', help='directory to write regular databases to', dest='standard_dir')

args = parser.parse_args()

test_list = [
    'test/sql/storage/compression/compression_selection.test',
    'test/sql/storage/compression/simple_compression.test',
    'test/sql/storage/test_store_deletes.test',
    'test/sql/storage/test_store_nulls_strings.test',
    'test/sql/storage/test_store_null_updates.test',
    'test/sql/storage/test_store_integers.test',
    'test/sql/storage/test_update_delete_string.test'
]

def run_test(args):
    res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    if res.returncode != 0:
        print("Failed to run test!")
        print("----------STDOUT-----------")
        print(stdout)
        print("----------STDERR-----------")
        print(stderr)
        print("---------------------")
        exit(1)

header_size = 4096 * 3
block_size = 262144

def compare_database(standard_db, zero_init_db):
    with open(standard_db, 'rb') as f:
        standard_data = f.read()
    with open(zero_init_db, 'rb') as f:
        zero_data = f.read()
    if len(standard_data) != len(zero_data):
        print(f"FAIL - Length mismatch between database {standard_db} ({str(len(standard_data))}) and {zero_init_db} ({str(len(zero_data))})")
        exit(1)
    for i in range(len(standard_data)):
        if standard_data[i] != zero_data[i]:
            print(f"FAIL - Mismatch between standard database ({standard_data[i]}) and zero-initialized database ({zero_data[i]}) at byte position {i}")
            if i < header_size:
                print("This byte is in the initial headers of the file")
            else:
                print(f"This byte is in block id {(i - header_size) // block_size}")
            print("This likely means that memory was not correctly zero-initialized in a block before being written out to disk")
            exit(1)
    print("Success!")

def compare_files(standard_dir, zero_init_dir):
    standard_list = os.listdir(standard_dir)
    zero_init_list = os.listdir(zero_init_dir)
    standard_list.sort()
    zero_init_list.sort()
    if standard_list != zero_init_list:
        print(f"FAIL - Directories contain mismatching files (standard - {str(standard_list)}, zero init - {str(zero_init_list)})")
        exit(1)
    if len(standard_list) == 0:
        print("FAIL - Directory is empty!")
        exit(1)
    for entry in standard_list:
        compare_database(os.path.join(standard_dir, entry), os.path.join(zero_init_dir, entry))


for test in test_list:
    print(f"Running test {test}")
    shutil.rmtree(args.standard_dir)
    shutil.rmtree(args.zero_init_dir)
    standard_args = [args.unittest, '--test-temp-dir', args.standard_dir, test, '--one-initialize']
    zero_init_args = [args.unittest, '--test-temp-dir', args.zero_init_dir, '--zero-initialize', test]
    print(f"Running test in one-initialize mode")
    run_test(standard_args)
    print(f"Running test in zero-initialize mode")
    run_test(zero_init_args)
    compare_files(args.standard_dir, args.zero_init_dir)
