import argparse
import os
import platform
import shutil
import sys

so_suffix = '.dylib' if platform.system()=='Darwin' else '.so'

parser = argparse.ArgumentParser(description='Run the psqlodbc program.')
parser.add_argument('--build_psqlodbc', dest='build_psqlodbc',
                     action='store_const', const=True, help='clone and build psqlodbc')
parser.add_argument('--psqlodbc', dest='psqlodbcdir',
                     default=os.path.join(os.getcwd(), 'psqlodbc'), help='path of the psqlodbc directory')
parser.add_argument('--odbc_lib', dest='odbclib',
                     default=None, help='path of the odbc .so or .dylib file')
parser.add_argument('--release', dest='release', action='store_const', const=True, help='Specify to use release mode instead of debug mode')
parser.add_argument('--overwrite', dest='overwrite', action='store_const', const=True, help='Whether or not to overwrite the ~/.odbc.ini and ~/.odbcinst.ini files')
parser.add_argument('--fix', dest='fix', action='store_const', const=True, help='Whether or not to fix tests, or whether to just run them')
parser.add_argument('--test', dest='test', default=None, help='A specific test to run (if any)')
parser.add_argument('--trace_file', dest='tracefile', default='/tmp/odbctrace', help='Path to tracefile of ODBC script')
parser.add_argument('--duckdb_dir', dest='duckdbdir', default=os.getcwd(), help='Path to DuckDB directory')
parser.add_argument('--no-trace', dest='notrace', action='store_const', const=True, help='Do not print trace')
parser.add_argument('--no-exit', dest='noexit', action='store_const', const=True, help='Do not exit on test failure')
parser.add_argument('--debugger', dest='debugger', default=None, choices=['lldb', 'gdb'], help='Debugger to attach (if any). If set, will set up the environment and give you a command to run with the debugger.')


args = parser.parse_args()

def print_trace_and_exit():
    if args.notrace is None:
        with open(args.tracefile, 'rb') as f:
            print(f.read().decode('utf8', 'ignore'))
    if args.noexit is None:
        exit(1)

def syscall(arg, error, print_trace=True):
    ret = os.system(arg)
    if ret != 0:
        print(error)
        if print_trace:
            print_trace_and_exit()
        exit(1)


build_config = 'debug'
if args.release is not None:
    build_config = 'release'

if args.build_psqlodbc is not None:
    if not os.path.isdir('psqlodbc'):
        syscall('git clone git@github.com:Mytherin/psqlodbc.git', 'Failed to clone psqlodbc', False)
    else:
        syscall('(cd psqlodbc && git pull)', 'Failed to pull latest changes for psqlodbc')
    syscall(f'(cd psqlodbc && make {build_config})', 'Failed to build psqlodbc', False)

if not os.path.isdir(args.psqlodbcdir):
    print(f'ERROR: Could not find path to psqlodbc {args.psqlodbcdir}, specify the --psqlodbc [dir] option')
    exit(1)

if args.overwrite:
    odbc_lib = args.odbclib
    if odbc_lib is None:
        odbc_lib = os.path.join(os.getcwd(), 'build', build_config, 'tools', 'odbc', 'libduckdb_odbc' + so_suffix)
    if not os.path.isfile(odbc_lib):
        print(f'ERROR: Could not find the library file {odbc_lib}, specify the --odbc_lib [file] option')
        exit(1)

    odbcinst_ini = f'''[ODBC]
Trace = yes
TraceFile = {args.tracefile}

[DuckDB Driver]
Driver = {odbc_lib}
'''

    odbc_ini = f'''[DuckDB]
Driver = DuckDB Driver
Database=:memory:
'''
    user_dir = os.path.expanduser('~')
    with open(os.path.join(user_dir, '.odbcinst.ini'), 'w+') as f:
        f.write(odbcinst_ini)

    with open(os.path.join(user_dir, '.odbc.ini'), 'w+') as f:
        f.write(odbc_ini)

def try_remove(fpath):
    try:
        os.remove(fpath)
    except:
        pass

os.chdir(args.psqlodbcdir)

os.environ['PSQLODBC_TEST_DSN'] = 'DuckDB'

try_remove(args.tracefile)
try_remove(os.path.join(args.psqlodbcdir, 'contrib_regression'))
try_remove(os.path.join(args.psqlodbcdir, 'contrib_regression.wal'))

odbc_build_dir = os.path.join(args.psqlodbcdir, 'build', build_config)
odbc_reset = os.path.join(odbc_build_dir, 'reset-db')
odbc_test = os.path.join(odbc_build_dir, 'psql_odbc_test')
test_list_file = os.path.join(args.duckdbdir, 'tools', 'odbc', 'test', 'psql_supported_tests')

syscall(odbc_reset + ' < sampletables.sql', 'Failed to reset db')

test_list = []
if args.test is None:
    # all tests: read from list
    with open(test_list_file, 'r') as f:
        test_list = [x.strip() for x in f.readlines() if len(x.strip()) > 0]
else:
    test_list = [args.test]

if args.debugger is not None:
    argstart = '--args'
    if args.debugger == 'lldb':
        argstart = '--'
    print("=============================================")
    print("Run the following command to start a debugger")
    print("=============================================")
    print(f"{args.debugger} {argstart} {odbc_test} {test_list[0]}")
    exit(0)

for test in test_list:
    fix_suffix = ' --fix' if args.fix is not None else ''
    print(f"Running test {test}")
    syscall(odbc_test + ' ' + test + fix_suffix, f"Failed to run test {test}")

