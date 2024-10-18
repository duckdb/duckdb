import os
import sys
import shutil
import subprocess
from python_helpers import open_utf8
import re

excluded_objects = ['utf8proc_data.cpp']


def third_party_includes():
    includes = []
    includes += [os.path.join('third_party', 'concurrentqueue')]
    includes += [os.path.join('third_party', 'fast_float')]
    includes += [os.path.join('third_party', 'fastpforlib')]
    includes += [os.path.join('third_party', 'fmt', 'include')]
    includes += [os.path.join('third_party', 'fsst')]
    includes += [os.path.join('third_party', 'httplib')]
    includes += [os.path.join('third_party', 'hyperloglog')]
    includes += [os.path.join('third_party', 'jaro_winkler')]
    includes += [os.path.join('third_party', 'jaro_winkler', 'details')]
    includes += [os.path.join('third_party', 'libpg_query')]
    includes += [os.path.join('third_party', 'libpg_query', 'include')]
    includes += [os.path.join('third_party', 'lz4')]
    includes += [os.path.join('third_party', 'brotli', 'include')]
    includes += [os.path.join('third_party', 'brotli', 'common')]
    includes += [os.path.join('third_party', 'brotli', 'dec')]
    includes += [os.path.join('third_party', 'brotli', 'enc')]
    includes += [os.path.join('third_party', 'mbedtls')]
    includes += [os.path.join('third_party', 'mbedtls', 'include')]
    includes += [os.path.join('third_party', 'mbedtls', 'library')]
    includes += [os.path.join('third_party', 'miniz')]
    includes += [os.path.join('third_party', 'pcg')]
    includes += [os.path.join('third_party', 're2')]
    includes += [os.path.join('third_party', 'skiplist')]
    includes += [os.path.join('third_party', 'tdigest')]
    includes += [os.path.join('third_party', 'utf8proc')]
    includes += [os.path.join('third_party', 'utf8proc', 'include')]
    includes += [os.path.join('third_party', 'yyjson', 'include')]
    return includes


def third_party_sources():
    sources = []
    sources += [os.path.join('third_party', 'fmt')]
    sources += [os.path.join('third_party', 'fsst')]
    sources += [os.path.join('third_party', 'miniz')]
    sources += [os.path.join('third_party', 're2')]
    sources += [os.path.join('third_party', 'hyperloglog')]
    sources += [os.path.join('third_party', 'skiplist')]
    sources += [os.path.join('third_party', 'fastpforlib')]
    sources += [os.path.join('third_party', 'utf8proc')]
    sources += [os.path.join('third_party', 'libpg_query')]
    sources += [os.path.join('third_party', 'mbedtls')]
    sources += [os.path.join('third_party', 'yyjson')]
    return sources


def file_is_lib(fname, libname):
    libextensions = ['.a', '.lib']
    libprefixes = ['', 'lib']
    for ext in libextensions:
        for prefix in libprefixes:
            potential_libname = prefix + libname + ext
            if fname == potential_libname:
                return True
    return False


def get_libraries(binary_dir, libraries, extensions):
    result_libs = []

    def find_library_recursive(search_dir, libname):
        flist = os.listdir(search_dir)
        for fname in flist:
            fpath = os.path.join(search_dir, fname)
            if os.path.isdir(fpath):
                entry = find_library_recursive(fpath, libname)
                if entry != None:
                    return entry
            elif os.path.isfile(fpath) and file_is_lib(fname, libname):
                return search_dir
        return None

    def find_library(search_dir, libname, result_libs, required=False):
        if libname == 'Threads::Threads':
            result_libs += [(None, 'pthread')]
            return
        libdir = find_library_recursive(binary_dir, libname)
        if libdir is None and required:
            raise Exception(f"Failed to locate required library {libname} in {binary_dir}")

        result_libs += [(libdir, libname)]

    duckdb_lib_name = 'duckdb_static'
    if os.name == 'nt':
        duckdb_lib_name = 'duckdb'
    find_library(os.path.join(binary_dir, 'src'), duckdb_lib_name, result_libs, True)
    for ext in extensions:
        find_library(os.path.join(binary_dir, 'extension', ext), ext + '_extension', result_libs, True)

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


def get_git_describe():
    override_git_describe = os.getenv('OVERRIDE_GIT_DESCRIBE') or ''
    # empty override_git_describe, either since env was empty string or not existing
    # -> ask git (that can fail, so except in place)
    if len(override_git_describe) == 0:
        try:
            return subprocess.check_output(['git', 'describe', '--tags', '--long']).strip().decode('utf8')
        except subprocess.CalledProcessError:
            return "v0.0.0-0-gdeadbeeff"
    if len(override_git_describe.split('-')) == 3:
        return override_git_describe
    if len(override_git_describe.split('-')) == 1:
        override_git_describe += "-0"
    assert len(override_git_describe.split('-')) == 2
    try:
        return (
            override_git_describe
            + "-g"
            + subprocess.check_output(['git', 'log', '-1', '--format=%h']).strip().decode('utf8')
        )
    except subprocess.CalledProcessError:
        return override_git_describe + "-g" + "deadbeeff"


def git_commit_hash():
    if 'SETUPTOOLS_SCM_PRETEND_HASH' in os.environ:
        return os.environ['SETUPTOOLS_SCM_PRETEND_HASH']
    try:
        git_describe = get_git_describe()
        hash = git_describe.split('-')[2].lstrip('g')
        return hash
    except:
        return "deadbeeff"


def prefix_version(version):
    """Make sure the version is prefixed with 'v' to be of the form vX.Y.Z"""
    if version.startswith('v'):
        return version
    return 'v' + version


def git_dev_version():
    if 'SETUPTOOLS_SCM_PRETEND_VERSION' in os.environ:
        return prefix_version(os.environ['SETUPTOOLS_SCM_PRETEND_VERSION'])
    try:
        long_version = get_git_describe()
        version_splits = long_version.split('-')[0].lstrip('v').split('.')
        dev_version = long_version.split('-')[1]
        if int(dev_version) == 0:
            # directly on a tag: emit the regular version
            return "v" + '.'.join(version_splits)
        else:
            # not on a tag: increment the version by one and add a -devX suffix
            version_splits[2] = str(int(version_splits[2]) + 1)
            return "v" + '.'.join(version_splits) + "-dev" + dev_version
    except:
        return "v0.0.0"


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


def build_package(target_dir, extensions, linenumbers=False, unity_count=32, folder_name='duckdb', short_paths=False):
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
    dev_v_parts = dev_version.lstrip('v').split('.')
    os.chdir(curdir)
    # open the file and read the current contents
    fpath = os.path.join(target_dir, 'src', 'function', 'table', 'version', 'pragma_version.cpp')
    with open_utf8(fpath, 'r') as f:
        text = f.read()
    # now add the DUCKDB_SOURCE_ID define, if it is not there already
    found_hash = False
    found_dev = False
    found_major = False
    found_minor = False
    found_patch = False
    lines = text.split('\n')
    for i in range(len(lines)):
        if '#define DUCKDB_SOURCE_ID ' in lines[i]:
            lines[i] = '#define DUCKDB_SOURCE_ID "{}"'.format(githash)
            found_hash = True
        if '#define DUCKDB_VERSION ' in lines[i]:
            lines[i] = '#define DUCKDB_VERSION "{}"'.format(dev_version)
            found_dev = True
        if '#define DUCKDB_MAJOR_VERSION ' in lines[i]:
            lines[i] = '#define DUCKDB_MAJOR_VERSION {}'.format(int(dev_v_parts[0]))
            found_major = True
        if '#define DUCKDB_MINOR_VERSION ' in lines[i]:
            lines[i] = '#define DUCKDB_MINOR_VERSION {}'.format(int(dev_v_parts[1]))
            found_minor = True
        if '#define DUCKDB_PATCH_VERSION ' in lines[i]:
            lines[i] = '#define DUCKDB_PATCH_VERSION "{}"'.format(dev_v_parts[2])
            found_patch = True
    if not found_hash:
        lines = ['#ifndef DUCKDB_SOURCE_ID', '#define DUCKDB_SOURCE_ID "{}"'.format(githash), '#endif'] + lines
    if not found_dev:
        lines = ['#ifndef DUCKDB_VERSION', '#define DUCKDB_VERSION "{}"'.format(dev_version), '#endif'] + lines
    if not found_major:
        lines = [
            '#ifndef DUCKDB_MAJOR_VERSION',
            '#define DUCKDB_MAJOR_VERSION {}'.format(int(dev_v_parts[0])),
            '#endif',
        ] + lines
    if not found_minor:
        lines = [
            '#ifndef DUCKDB_MINOR_VERSION',
            '#define DUCKDB_MINOR_VERSION {}'.format(int(dev_v_parts[1])),
            '#endif',
        ] + lines
    if not found_patch:
        lines = [
            '#ifndef DUCKDB_PATCH_VERSION',
            '#define DUCKDB_PATCH_VERSION "{}"'.format(dev_v_parts[2]),
            '#endif',
        ] + lines
    text = '\n'.join(lines)
    with open_utf8(fpath, 'w+') as f:
        f.write(text)

    def file_is_excluded(fname):
        for entry in excluded_objects:
            if entry in fname:
                return True
        return False

    def generate_unity_build(entries, unity_name, linenumbers):
        ub_file = os.path.join(target_dir, unity_name)
        with open_utf8(ub_file, 'w+') as f:
            for entry in entries:
                if linenumbers:
                    f.write('#line 0 "{}"\n'.format(convert_backslashes(entry)))
                f.write('#include "{}"\n\n'.format(convert_backslashes(entry)))
        return ub_file

    def generate_unity_builds(source_list, nsplits, linenumbers):
        files_per_directory = {}
        for source in source_list:
            dirname = os.path.dirname(source)
            if dirname not in files_per_directory:
                files_per_directory[dirname] = []
            files_per_directory[dirname].append(source)

        new_source_files = []
        for dirname in files_per_directory.keys():
            current_files = files_per_directory[dirname]
            cmake_file = os.path.join(dirname, 'CMakeLists.txt')
            unity_build = False
            if os.path.isfile(cmake_file):
                with open(cmake_file, 'r') as f:
                    text = f.read()
                    if 'add_library_unity' in text:
                        unity_build = True
                        # re-order the files in the unity build so that they follow the same order as the CMake
                        scores = {}
                        filenames = [x[0] for x in re.findall('([a-zA-Z0-9_]+[.](cpp|cc|c|cxx))', text)]
                        score = 0
                        for filename in filenames:
                            scores[filename] = score
                            score += 1
                        current_files.sort(
                            key=lambda x: scores[os.path.basename(x)] if os.path.basename(x) in scores else 99999
                        )
            if not unity_build:
                if short_paths:
                    # replace source files with "__"
                    for file in current_files:
                        unity_filename = os.path.basename(file)
                        new_source_files.append(generate_unity_build([file], unity_filename, linenumbers))
                else:
                    # directly use the source files
                    new_source_files += [os.path.join(folder_name, file) for file in current_files]
            else:
                unity_base = dirname.replace(os.path.sep, '_')
                unity_name = f'ub_{unity_base}.cpp'
                new_source_files.append(generate_unity_build(current_files, unity_name, linenumbers))
        return new_source_files

    original_sources = source_list
    source_list = generate_unity_builds(source_list, unity_count, linenumbers)

    os.chdir(prev_wd)
    return (
        [convert_backslashes(x) for x in source_list if not file_is_excluded(x)],
        [convert_backslashes(x) for x in include_list],
        [convert_backslashes(x) for x in original_sources],
    )
