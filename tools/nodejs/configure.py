import os
import sys
import json
import pickle

# list of extensions to bundle
extensions = ['parquet', 'icu', 'json']

# path to target
basedir = os.getcwd()
target_dir = os.path.join(basedir, 'src', 'duckdb')
gyp_in = os.path.join(basedir, 'binding.gyp.in')
gyp_out = os.path.join(basedir, 'binding.gyp')
cache_file = os.path.join(basedir, 'filelist.cache')

# path to package_build.py
os.chdir(os.path.join('..', '..'))
scripts_dir = 'scripts'

sys.path.append(scripts_dir)
import package_build

defines = ['DUCKDB_EXTENSION_{}_LINKED'.format(ext.upper()) for ext in extensions]

# Autoloading is on by default for node distributions
defines.extend(['DUCKDB_EXTENSION_AUTOLOAD_DEFAULT=1', 'DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT=1'])

if os.environ.get('DUCKDB_NODE_BUILD_CACHE') == '1' and os.path.isfile(cache_file):
    with open(cache_file, 'rb') as f:
        cache = pickle.load(f)
    source_list = cache['source_list']
    include_list = cache['include_list']
    libraries = cache['libraries']
    windows_options = cache['windows_options']
    cflags = cache['cflags']
elif 'DUCKDB_NODE_BINDIR' in os.environ:

    def find_library_path(libdir, libname):
        flist = os.listdir(libdir)
        for fname in flist:
            fpath = os.path.join(libdir, fname)
            if os.path.isfile(fpath) and package_build.file_is_lib(fname, libname):
                return fpath
        raise Exception(f"Failed to find library {libname} in {libdir}")

    # existing build
    existing_duckdb_dir = os.environ['DUCKDB_NODE_BINDIR']
    cflags = os.environ['DUCKDB_NODE_CFLAGS']
    libraries = os.environ['DUCKDB_NODE_LIBS'].split(' ')

    include_directories = [os.path.join('..', '..', include) for include in package_build.third_party_includes()]
    include_list = package_build.includes(extensions)

    result_libraries = package_build.get_libraries(existing_duckdb_dir, libraries, extensions)
    libraries = []
    for libdir, libname in result_libraries:
        if libdir is None:
            continue
        libraries.append(find_library_path(libdir, libname))

    source_list = []
    cflags = []
    windows_options = []
    if os.name == 'nt':
        windows_options = [x for x in os.environ['DUCKDB_NODE_CFLAGS'].split(' ') if x.startswith('/')]
    else:
        if '-g' in os.environ['DUCKDB_NODE_CFLAGS']:
            cflags += ['-g']
        if '-O0' in os.environ['DUCKDB_NODE_CFLAGS']:
            cflags += ['-O0']
        if '-DNDEBUG' in os.environ['DUCKDB_NODE_CFLAGS']:
            defines += ['NDEBUG']

    if 'DUCKDB_NODE_BUILD_CACHE' in os.environ:
        cache = {
            'source_list': source_list,
            'include_list': include_list,
            'libraries': libraries,
            'cflags': cflags,
            'windows_options': windows_options,
        }
        with open(cache_file, 'wb+') as f:
            pickle.dump(cache, f)
else:
    # fresh build - copy over all of the files
    (source_list, include_list, original_sources) = package_build.build_package(target_dir, extensions, False)

    # # the list of all source files (.cpp files) that have been copied into the `duckdb_source_copy` directory
    # print(source_list)
    # # the list of all include files
    # print(include_list)
    source_list = [os.path.relpath(x, basedir) if os.path.isabs(x) else os.path.join('src', x) for x in source_list]
    include_list = [os.path.join('src', 'duckdb', x) for x in include_list]
    libraries = []
    windows_options = ['/GR']
    cflags = ['-frtti']


def sanitize_path(x):
    return x.replace('\\', '/')


source_list = [sanitize_path(x) for x in source_list]
include_list = [sanitize_path(x) for x in include_list]
libraries = [sanitize_path(x) for x in libraries]

with open(gyp_in, 'r') as f:
    input_json = json.load(f)


def replace_entries(node, replacement_map):
    if type(node) == type([]):
        for key in replacement_map.keys():
            if key in node:
                node.remove(key)
                node += replacement_map[key]
        for entry in node:
            if type(entry) == type([]) or type(entry) == type({}):
                replace_entries(entry, replacement_map)
    if type(node) == type({}):
        for key in node.keys():
            replace_entries(node[key], replacement_map)


replacement_map = {}
replacement_map['${SOURCE_FILES}'] = source_list
replacement_map['${INCLUDE_FILES}'] = include_list
replacement_map['${DEFINES}'] = defines
replacement_map['${LIBRARY_FILES}'] = libraries
replacement_map['${CFLAGS}'] = cflags
replacement_map['${WINDOWS_OPTIONS}'] = windows_options

replace_entries(input_json, replacement_map)

with open(gyp_out, 'w+') as f:
    json.dump(input_json, f, indent=4, separators=(", ", ": "))
