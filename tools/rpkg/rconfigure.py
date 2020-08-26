import os
import sys
import shutil
import subprocess

# DUCKDB_R_BINDIR
# DUCKDB_R_CFLAGS
# DUCKDB_R_LIBS
if 'DUCKDB_R_BINDIR' in os.environ and 'DUCKDB_R_CFLAGS' in os.environ and 'DUCKDB_R_LIBS' in os.environ:
    existing_duckdb_dir = os.environ['DUCKDB_R_BINDIR']
    compile_flags = os.environ['DUCKDB_R_CFLAGS'].replace('\\', '').replace('  ', ' ')
    libraries = [x for x in os.environ['DUCKDB_R_LIBS'].split(' ') if len(x) > 0]

    # use existing installation: set up Makevars
    with open(os.path.join('src', 'Makevars.in'), 'r') as f:
        text = f.read()

    def find_library_recursive(search_dir, potential_libnames):
        flist = os.listdir(search_dir)
        for fname in flist:
            fpath = os.path.join(search_dir, fname)
            if os.path.isdir(fpath):
                entry = find_library_recursive(fpath, potential_libnames)
                if entry != None:
                    return entry
            elif os.path.isfile(fpath) and fname in potential_libnames:
                return (search_dir, fname.split)
        return None

    def find_library(search_dir, libname, libnames, library_dirs):
        if libname == 'Threads::Threads':
            library_dirs += [None]
            libnames += ['pthread']
            return
        libextensions = ['.a', '.lib']
        libprefixes = ['', 'lib']
        potential_libnames = []
        for ext in libextensions:
            for prefix in libprefixes:
                potential_libnames.append(prefix + libname + ext)
        (libdir, _) = find_library_recursive(existing_duckdb_dir, potential_libnames)

        libnames += [libname]
        library_dirs += [libdir]

    libnames = ['duckdb_static', 'parquet_extension', 'icu_extension']

    library_dirs = []
    library_dirs += [os.path.join(existing_duckdb_dir, 'src')]
    library_dirs += [os.path.join(existing_duckdb_dir, 'extension', 'parquet')]
    library_dirs += [os.path.join(existing_duckdb_dir, 'extension', 'icu')]

    # add includes for duckdb and parquet
    compile_flags += ' -I' + os.path.join(os.getcwd(), '..', '..', 'src', 'include')
    compile_flags += ' -I' + os.path.join(os.getcwd(), '..', '..', 'extension', 'parquet', 'include')

    for libname in libraries:
        find_library(existing_duckdb_dir, libname, libnames, library_dirs)

    if len(library_dirs) != len(libnames):
        print("Library dirs and libnames should match!")
        exit(1)

    link_flags = ''
    for i in range(len(library_dirs)):
        libname = libnames[i]
        libdir = library_dirs[i]
        if libdir != None:
            link_flags += ' -L' + libdir
        if libname != None:
            link_flags += ' -l' + libname

    text = text.replace('{{ SOURCES }}', '')
    text = text.replace('{{ INCLUDES }}', compile_flags.strip())
    text = text.replace('{{ LINK_FLAGS }}', link_flags.strip())

    # now write it to the output Makevars
    with open(os.path.join('src', 'Makevars'), 'w+') as f:
        f.write(text)
    exit(0)

excluded_objects = ['utf8proc_data.cpp']

if not os.path.isfile(os.path.join('..', '..', 'scripts', 'amalgamation.py')):
    print("Could not find amalgamation script! This script needs to be launched from the subdirectory tools/rpkg")
    exit(1)

target_dir = os.path.join(os.getcwd(), 'src', 'duckdb')

if not os.path.isdir(target_dir):
    os.mkdir(target_dir)

prev_wd = os.getcwd()
os.chdir(os.path.join('..', '..'))

# read the source id
proc = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
githash = proc.stdout.read().strip()


# obtain the list of source files from the amalgamation
sys.path.append('scripts')
import amalgamation
source_list = amalgamation.list_sources()
include_list = amalgamation.list_include_dirs()
include_files = amalgamation.list_includes()

def copy_file(src, target_dir):
    # get the path
    full_path = src.split(os.path.sep)
    current_path = target_dir
    for i in range(len(full_path) - 1):
        current_path = os.path.join(current_path, full_path[i])
        if not os.path.isdir(current_path):
            os.mkdir(current_path)
    amalgamation.copy_if_different(src, os.path.join(current_path, full_path[-1]))


# now do the same for the parquet extension
sys.path.append(os.path.join('extension', 'parquet'))
import parquet_amalgamation
parquet_include_directories = parquet_amalgamation.include_directories

include_files += amalgamation.list_includes_files(parquet_include_directories)

include_list += parquet_include_directories
source_list += parquet_amalgamation.source_files

for src in source_list:
    copy_file(src, target_dir)

for inc in include_files:
    copy_file(inc, target_dir)

def file_is_excluded(fname):
    for entry in excluded_objects:
        if entry in fname:
            return True
    return False

def convert_backslashes(x):
    return '/'.join(x.split(os.path.sep))

def generate_unity_build(entries, idx):
    ub_file = os.path.join(target_dir, 'amalgamation-{}.cpp'.format(str(idx)))
    with open(ub_file, 'w+') as f:
        for entry in entries:
            f.write('#line 0 "{}"\n'.format(convert_backslashes(entry)))
            f.write('#include "{}"\n\n'.format(convert_backslashes(entry)))
    return ub_file

def generate_unity_builds(source_list, nsplits):
    source_list.sort()

    files_per_split = len(source_list) / nsplits
    new_source_files = []
    current_files = []
    idx = 1
    for entry in source_list:
        if not entry.startswith('src'):
            new_source_files.append(os.path.join('duckdb', entry))
            continue

        current_files.append(entry)
        if len(current_files) > files_per_split:
            new_source_files.append(generate_unity_build(current_files, idx))
            current_files = []
            idx += 1
    if len(current_files) > 0:
        new_source_files.append(generate_unity_build(current_files, idx))
        current_files = []
        idx += 1

    return new_source_files

source_list = generate_unity_builds(source_list, 8)

# object list
object_list = ' '.join([x.rsplit('.', 1)[0] + '.o' for x in source_list if not file_is_excluded(x)])
# include list
include_list = ' '.join(['-I' + os.path.join('duckdb', x) for x in include_list])
include_list += ' -Iduckdb'

os.chdir(prev_wd)

# read Makevars.in and replace the {{ SOURCES }} and {{ INCLUDES }} macros
with open(os.path.join('src', 'Makevars.in'), 'r') as f:
    text = f.read()

text = text.replace('{{ SOURCES }}', convert_backslashes(object_list))
text = text.replace('{{ INCLUDES }}', convert_backslashes(include_list) + ' -DDUCKDB_SOURCE_ID=\\"{}\\"'.format(githash.decode('utf8')))
text = text.replace('{{ LINK_FLAGS }}', '')

# now write it to the output Makevars
with open(os.path.join('src', 'Makevars'), 'w+') as f:
    f.write(text)
