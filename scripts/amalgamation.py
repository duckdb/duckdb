# these are the remnants of the once-proud amalgamation.py
# we still use those in some places, e.g. test_compile.py and the swift build

import package_build
import os
from python_helpers import open_utf8, normalize_path
import shutil

src_dir = 'src'
compile_directories = [src_dir] + package_build.third_party_sources() + ['extension/loader']
include_dir = os.path.join(src_dir, 'include')
include_paths = [include_dir] + package_build.third_party_includes()
excluded_files = ['grammar.cpp', 'grammar.hpp', 'symbols.cpp']
# files excluded from individual file compilation during test_compile
excluded_compilation_files = excluded_files + ['gram.hpp', 'kwlist.hpp', "duckdb-c.cpp"]
amal_dir = os.path.join('src', 'amalgamation')

# files always excluded
always_excluded = []
written_files = {}

def need_to_write_file(current_file, ignore_excluded=False):
    if amal_dir in current_file:
        return False
    if current_file in always_excluded:
        return False
    if current_file.split(os.sep)[-1] in excluded_files and not ignore_excluded:
        # file is in ignored files set
        return False
    if current_file in written_files:
        # file is already written
        return False
    return True


def list_files(dname, file_list):
    files = os.listdir(dname)
    files.sort()
    for fname in files:
        if fname in excluded_files:
            continue
        fpath = os.path.join(dname, fname)
        if os.path.isdir(fpath):
            list_files(fpath, file_list)
        elif fname.endswith(('.cpp', '.c', '.cc')):
            if need_to_write_file(fpath):
                file_list.append(fpath)

def list_sources():
    file_list = []
    for compile_dir in compile_directories:
        list_files(compile_dir, file_list)
    return file_list


def list_include_files_recursive(dname, file_list):
    files = os.listdir(dname)
    files.sort()
    for fname in files:
        if fname in excluded_files:
            continue
        fpath = os.path.join(dname, fname)
        if os.path.isdir(fpath):
            list_include_files_recursive(fpath, file_list)
        elif fname.endswith(('.hpp', '.ipp', '.h', '.hh', '.tcc', '.inc')):
            file_list.append(fpath)


def list_includes_files(include_dirs):
    file_list = []
    for include_dir in include_dirs:
        list_include_files_recursive(include_dir, file_list)
    return file_list



def copy_if_different(src, dest):
    if os.path.isfile(dest):
        # dest exists, check if the files are different
        with open_utf8(src, 'r') as f:
            source_text = f.read()
        with open_utf8(dest, 'r') as f:
            dest_text = f.read()
        if source_text == dest_text:
            # print("Skipping copy of " + src + ", identical copy already exists at " + dest)
            return
    # print("Copying " + src + " to " + dest)
    shutil.copyfile(src, dest)


def list_includes():
    return list_includes_files(include_paths)

def list_include_dirs():
    return include_paths