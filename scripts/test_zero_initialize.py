import os
import argparse
import subprocess
import shutil

parser = argparse.ArgumentParser(
    description='''Runs storage tests both with explicit one-initialization and with explicit zero-initialization, and verifies that the final storage files are the same.
The purpose of this is to verify all memory is correctly initialized before writing to disk - which prevents leaking of in-memory data in storage files by writing uninitialized memory to disk.'''
)
parser.add_argument('--unittest', default='build/debug/test/unittest', help='path to unittest', dest='unittest')
parser.add_argument(
    '--zero_init_dir',
    default='test_zero_init_db',
    help='directory to write zero-initialized databases to',
    dest='zero_init_dir',
)
parser.add_argument(
    '--standard_dir', default='test_standard_db', help='directory to write regular databases to', dest='standard_dir'
)

args = parser.parse_args()

test_list = [
    'test/sql/index/art/storage/test_art_checkpoint.test',
    'test/sql/storage/compression/simple_compression.test',
    'test/sql/storage/delete/test_store_deletes.test',
    'test/sql/storage/mix/test_update_delete_string.test',
    'test/sql/storage/nested/struct_of_lists_unaligned.test',
    'test/sql/storage/test_store_integers.test',
    'test/sql/storage/test_store_nulls_strings.test',
    'test/sql/storage/update/test_store_null_updates.test',
]


def run_test(args):
    res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    if res.returncode != 0:
        print("Failed to run test!")
        print("----------COMMAND-----------")
        print(' '.join(args))
        print("----------STDOUT-----------")
        print(stdout)
        print("----------STDERR-----------")
        print(stderr)
        print("---------------------")
        exit(1)


header_size = 4096 * 3
block_size = 262144
checksum_size = 8


def handle_error(i, standard_db, zero_init_db, standard_data, zero_data):
    print("------------------------------------------------------------------")
    print(f"FAIL - Mismatch between one-initialized and zero-initialized databases at byte position {i}")
    print("------------------------------------------------------------------")
    print(f"One-initialized database {standard_db} - byte value {standard_data}")
    print(f"Zero-initialized database {zero_init_db} - byte value {zero_data}")
    if i < header_size:
        print("This byte is in the initial headers of the file")
    else:
        byte_pos = (i - header_size) % block_size
        if byte_pos >= checksum_size:
            print(
                f"This byte is in block id {(i - header_size) // block_size} at byte position {byte_pos - checksum_size} (position {byte_pos} including the block checksum)"
            )
        else:
            print(f"This byte is in block id {(i - header_size) // block_size} at byte position {byte_pos}")
            print("This is in the checksum part of the block")
    print("------------------------------------------------------------------")
    print(
        "This error likely means that memory was not correctly zero-initialized in a block before being written out to disk."
    )


def compare_database(standard_db, zero_init_db):
    with open(standard_db, 'rb') as f:
        standard_data = f.read()
    with open(zero_init_db, 'rb') as f:
        zero_data = f.read()
    if len(standard_data) != len(zero_data):
        print(
            f"FAIL - Length mismatch between database {standard_db} ({str(len(standard_data))}) and {zero_init_db} ({str(len(zero_data))})"
        )
        return False
    found_error = None
    for i in range(len(standard_data)):
        if standard_data[i] != zero_data[i]:
            if i > header_size:
                byte_pos = (i - header_size) % block_size
                if byte_pos <= 8:
                    # different checksum, skip because it does not tell us anything!
                    if found_error is None:
                        found_error = i
                    continue
            handle_error(i, standard_db, zero_init_db, standard_data[i], zero_data[i])
            return False
    if found_error is not None:
        i = found_error
        handle_error(i, standard_db, zero_init_db, standard_data[i], zero_data[i])
        return False
    print("Success!")
    return True


def compare_files(standard_dir, zero_init_dir):
    standard_list = os.listdir(standard_dir)
    zero_init_list = os.listdir(zero_init_dir)
    standard_list.sort()
    zero_init_list.sort()
    if standard_list != zero_init_list:
        print(
            f"FAIL - Directories contain mismatching files (standard - {str(standard_list)}, zero init - {str(zero_init_list)})"
        )
        return False
    if len(standard_list) == 0:
        print("FAIL - Directory is empty!")
        return False
    success = True
    for entry in standard_list:
        if not compare_database(os.path.join(standard_dir, entry), os.path.join(zero_init_dir, entry)):
            success = False
    return success


def clear_directories(directories):
    for dir in directories:
        try:
            shutil.rmtree(dir)
        except FileNotFoundError as e:
            pass


test_dirs = [args.standard_dir, args.zero_init_dir]

success = True
for test in test_list:
    print(f"Running test {test}")
    clear_directories(test_dirs)
    standard_args = [args.unittest, '--test-temp-dir', args.standard_dir, '--one-initialize', '--single-threaded', test]
    zero_init_args = [
        args.unittest,
        '--test-temp-dir',
        args.zero_init_dir,
        '--zero-initialize',
        '--single-threaded',
        test,
    ]
    print(f"Running test in one-initialize mode")
    run_test(standard_args)
    print(f"Running test in zero-initialize mode")
    run_test(zero_init_args)
    if not compare_files(args.standard_dir, args.zero_init_dir):
        success = False

clear_directories(test_dirs)

if not success:
    exit(1)
