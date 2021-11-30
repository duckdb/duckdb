import os
import sys
import shutil
import subprocess
from python_helpers import open_utf8

excluded_objects = ['utf8proc_data.cpp']

def get_libraries(binary_dir, libraries, extensions):
    result_libs = []
    def find_library_recursive(search_dir, potential_libnames):
        flist = os.listdir(search_dir)
        for fname in flist:
            fpath = os.path.join(search_dir, fname)
            if os.path.isdir(fpath):
                entry = find_library_recursive(fpath, potential_libnames)
                if entry != None:
                    return entry
            elif os.path.isfile(fpath) and fname in potential_libnames:
                return search_dir
        return None

    def find_library(search_dir, libname, result_libs):
        if libname == 'Threads::Threads':
            result_libs += [(None, 'pthread')]
            return
        libextensions = ['.a', '.lib']
        libprefixes = ['', 'lib']
        potential_libnames = []
        for ext in libextensions:
            for prefix in libprefixes:
                potential_libnames.append(prefix + libname + ext)
        libdir = find_library_recursive(binary_dir, potential_libnames)

        result_libs += [(libdir, libname)]

    result_libs += [(os.path.join(binary_dir, 'src'), 'duckdb_static')]
    for ext in extensions:
        result_libs += [(os.path.join(binary_dir, 'extension', ext), ext + '_extension')]

    for libname in libraries:
        find_library(binary_dir, libname, result_libs)

    return result_libs

def includes(extensions):
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    # add includes for duckdb and extensions
    includes = []
    includes.append(os.path.join(scripts_dir, '..', 'src', 'include'))
    includes.append(os.path.join(scripts_dir, '..'))
    includes.append(os.path.join(scripts_dir, '..', 'third_party', 'utf8proc', 'include'))
    for ext in extensions:
        includes.append(os.path.join(scripts_dir, '..', 'extension', ext, 'include'))
    return includes

def include_flags(extensions):
    return ' ' + ' '.join(['-I' + x for x in includes(extensions)])

def convert_backslashes(x):
    return '/'.join(x.split(os.path.sep))

def get_relative_path(source_dir, target_file):
    source_dir = convert_backslashes(source_dir)
    target_file = convert_backslashes(target_file)

    # absolute path: try to convert
    if source_dir in target_file:
        target_file = target_file.replace(source_dir, "").lstrip('/')
    return target_file

def git_commit_hash():
    try:
        return subprocess.check_output(['git','log','-1','--format=%h']).strip().decode('utf8')
    except:
        if 'SETUPTOOLS_SCM_PRETEND_HASH' in os.environ:
            return os.environ['SETUPTOOLS_SCM_PRETEND_HASH']
        else:
            return "deadbeeff"

def git_dev_version():
    try:
        version = subprocess.check_output(['git','describe','--tags','--abbrev=0']).strip().decode('utf8')
        long_version = subprocess.check_output(['git','describe','--tags','--long']).strip().decode('utf8')
        version_splits = version.lstrip('v').split('.')
        dev_version = long_version.split('-')[1]
        if int(dev_version) == 0:
            # directly on a tag: emit the regular version
            return '.'.join(version_splits)
        else:
            # not on a tag: increment the version by one and add a -devX suffix
            version_splits[2] = str(int(version_splits[2]) + 1)
            return '.'.join(version_splits) + "-dev" + dev_version
    except:
        if 'SETUPTOOLS_SCM_PRETEND_VERSION' in os.environ:
            return os.environ['SETUPTOOLS_SCM_PRETEND_VERSION']
        else:
            return "0.0.0"

def include_package(pkg_name, pkg_dir, include_files, include_list, source_list):
    import amalgamation
    original_path = sys.path
    # append the directory
    sys.path.append(pkg_dir)
    ext_pkg = __import__(pkg_name + '_config')

    ext_include_dirs = ext_pkg.include_directories
    ext_source_files = ext_pkg.source_files

    include_files += amalgamation.list_includes_files(ext_include_dirs)
    include_list += ext_include_dirs
    source_list += ext_source_files

    sys.path = original_path

def build_package(target_dir, extensions, linenumbers = False, unity_count = 32):
    if not os.path.isdir(target_dir):
        os.mkdir(target_dir)

    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(scripts_dir)
    import amalgamation

    prev_wd = os.getcwd()
    os.chdir(os.path.join(scripts_dir, '..'))

    # obtain the list of source files from the amalgamation
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
        target_name = full_path[-1]
        target_file = os.path.join(current_path, target_name)
        amalgamation.copy_if_different(src, target_file)

    # include the main extension helper
    include_files += [os.path.join('src', 'include', 'duckdb', 'main', 'extension_helper.hpp')]
    # include the separate extensions
    for ext in extensions:
        ext_path = os.path.join(scripts_dir, '..', 'extension', ext)
        include_package(ext, ext_path, include_files, include_list, source_list)

    for src in source_list:
        copy_file(src, target_dir)

    for inc in include_files:
        copy_file(inc, target_dir)

    # handle pragma_version.cpp: paste #define DUCKDB_SOURCE_ID and DUCKDB_VERSION there
    curdir = os.getcwd()
    os.chdir(os.path.join(scripts_dir, '..'))
    githash = git_commit_hash()
    dev_version = git_dev_version()
    os.chdir(curdir)
    # open the file and read the current contents
    fpath = os.path.join(target_dir, 'src', 'function', 'table', 'version', 'pragma_version.cpp')
    with open_utf8(fpath, 'r') as f:
        text = f.read()
    # now add the DUCKDB_SOURCE_ID define, if it is not there already
    found_hash = False
    found_dev = False
    lines = text.split('\n')
    for i in range(len(lines)):
        if '#define DUCKDB_SOURCE_ID ' in lines[i]:
            lines[i] = '#define DUCKDB_SOURCE_ID "{}"'.format(githash)
            found_hash = True
            break
        if '#define DUCKDB_VERSION ' in lines[i]:
            lines[i] = '#define DUCKDB_VERSION "{}"'.format(dev_version)
            found_dev = True
            break
    if not found_hash:
        lines = ['#ifndef DUCKDB_SOURCE_ID', '#define DUCKDB_SOURCE_ID "{}"'.format(githash), '#endif'] + lines
    if not found_dev:
        lines = ['#ifndef DUCKDB_VERSION', '#define DUCKDB_VERSION "{}"'.format(dev_version), '#endif'] + lines
    text = '\n'.join(lines)
    with open_utf8(fpath, 'w+') as f:
        f.write(text)

    def file_is_excluded(fname):
        for entry in excluded_objects:
            if entry in fname:
                return True
        return False

    def generate_unity_build(entries, idx, linenumbers):
        ub_file = os.path.join(target_dir, 'amalgamation-{}.cpp'.format(str(idx)))
        with open_utf8(ub_file, 'w+') as f:
            for entry in entries:
                if linenumbers:
                    f.write('#line 0 "{}"\n'.format(convert_backslashes(entry)))
                f.write('#include "{}"\n\n'.format(convert_backslashes(entry)))
        return ub_file

    def generate_unity_builds(source_list, nsplits, linenumbers):
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
                new_source_files.append(generate_unity_build(current_files, idx, linenumbers))
                current_files = []
                idx += 1
        if len(current_files) > 0:
            new_source_files.append(generate_unity_build(current_files, idx, linenumbers))
            current_files = []
            idx += 1

        return new_source_files

    original_sources = source_list
    if unity_count > 0:
        source_list = generate_unity_builds(source_list, unity_count, linenumbers)
    else:
        source_list = [os.path.join('duckdb', source) for source in source_list]

    os.chdir(prev_wd)
    return ([convert_backslashes(x) for x in source_list if not file_is_excluded(x)],
            [convert_backslashes(x) for x in include_list],
            [convert_backslashes(x) for x in original_sources])
