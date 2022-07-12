# this script creates a single header + source file combination out of the DuckDB sources
import os
import re
import sys
import shutil
import subprocess
from python_helpers import open_utf8, normalize_path

amal_dir = os.path.join('src', 'amalgamation')
header_file = os.path.join(amal_dir, "duckdb.hpp")
source_file = os.path.join(amal_dir, "duckdb.cpp")
temp_header = 'duckdb.hpp.tmp'
temp_source = 'duckdb.cpp.tmp'

skip_duckdb_includes = False

src_dir = 'src'
include_dir = os.path.join('src', 'include')
fmt_dir = os.path.join('third_party', 'fmt')
fmt_include_dir = os.path.join('third_party', 'fmt', 'include')
miniz_dir = os.path.join('third_party', 'miniz')
re2_dir = os.path.join('third_party', 're2')
pg_query_dir = os.path.join('third_party', 'libpg_query')
pg_query_include_dir = os.path.join('third_party', 'libpg_query', 'include')
hll_dir = os.path.join('third_party', 'hyperloglog')
fastpforlib_dir = os.path.join('third_party', 'fastpforlib')
tdigest_dir = os.path.join('third_party', 'tdigest')
utf8proc_dir = os.path.join('third_party', 'utf8proc')
utf8proc_include_dir = os.path.join('third_party', 'utf8proc', 'include')
httplib_include_dir = os.path.join('third_party', 'httplib')
fastfloat_include_dir = os.path.join('third_party', 'fast_float')
mbedtls_dir = os.path.join('third_party', 'mbedtls')
mbedtls_include_dir = os.path.join('third_party', 'mbedtls', 'include')
mbedtls_include_dir2 = os.path.join('third_party', 'mbedtls', 'library')
moodycamel_include_dir = os.path.join('third_party', 'concurrentqueue')
pcg_include_dir = os.path.join('third_party', 'pcg')

# files included in the amalgamated "duckdb.hpp" file
main_header_files = [os.path.join(include_dir, 'duckdb.hpp'),
    os.path.join(include_dir, 'duckdb.h'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'date.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'arrow.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'blob.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'decimal.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'hugeint.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'uuid.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'interval.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'timestamp.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'types', 'time.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'serializer', 'buffered_file_writer.hpp'),
    os.path.join(include_dir, 'duckdb', 'common', 'serializer', 'buffered_serializer.hpp'),
    os.path.join(include_dir, 'duckdb', 'main', 'appender.hpp'),
    os.path.join(include_dir, 'duckdb', 'main', 'client_context.hpp'),
    os.path.join(include_dir, 'duckdb', 'function', 'function.hpp'),
    os.path.join(include_dir, 'duckdb', 'function', 'table_function.hpp'),
    os.path.join(include_dir, 'duckdb', 'parser', 'parsed_data', 'create_table_function_info.hpp'),
    os.path.join(include_dir, 'duckdb', 'parser', 'parsed_data', 'create_copy_function_info.hpp')]
extended_amalgamation = False
if '--extended' in sys.argv:
    def add_include_dir(dirpath):
        return [os.path.join(dirpath, x) for x in os.listdir(dirpath)]

    extended_amalgamation = True
    main_header_files += [os.path.join(include_dir, x) for x in [
        'duckdb/planner/expression/bound_constant_expression.hpp',
        'duckdb/planner/expression/bound_function_expression.hpp',
        'duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp',
        'duckdb/parser/parsed_data/create_table_info.hpp',
        'duckdb/planner/parsed_data/bound_create_table_info.hpp',
        'duckdb/parser/constraints/not_null_constraint.hpp',
        'duckdb/storage/data_table.hpp',
        'duckdb/function/pragma_function.hpp',
        'duckdb/parser/qualified_name.hpp',
        'duckdb/parser/parser.hpp',
        'duckdb/planner/binder.hpp',
        'duckdb/storage/object_cache.hpp',
        'duckdb/planner/table_filter.hpp',
        "duckdb/storage/statistics/string_statistics.hpp",
        "duckdb/storage/statistics/numeric_statistics.hpp",
        "duckdb/planner/filter/conjunction_filter.hpp",
        "duckdb/planner/filter/constant_filter.hpp",
        "duckdb/execution/operator/persistent/buffered_csv_reader.hpp",
        "duckdb/common/types/vector_cache.hpp",
        "duckdb/planner/filter/null_filter.hpp",
        "duckdb/common/arrow_wrapper.hpp",
        "duckdb/common/hive_partitioning.hpp",
        "duckdb/common/compressed_file_system.hpp"]]
    main_header_files += add_include_dir(os.path.join(include_dir, 'duckdb/parser/expression'))
    main_header_files += add_include_dir(os.path.join(include_dir, 'duckdb/parser/parsed_data'))
    main_header_files += add_include_dir(os.path.join(include_dir, 'duckdb/parser/tableref'))
    main_header_files = normalize_path(main_header_files)

# include paths for where to search for include files during amalgamation
include_paths = [include_dir, fmt_include_dir, re2_dir, miniz_dir, utf8proc_include_dir, hll_dir, fastpforlib_dir, tdigest_dir, utf8proc_dir, pg_query_include_dir, pg_query_dir, moodycamel_include_dir, pcg_include_dir, httplib_include_dir, fastfloat_include_dir, mbedtls_include_dir, mbedtls_include_dir2, mbedtls_dir]
# paths of where to look for files to compile and include to the final amalgamation
compile_directories = [src_dir, fmt_dir, miniz_dir, re2_dir, hll_dir, fastpforlib_dir, utf8proc_dir, pg_query_dir, mbedtls_dir]

# files always excluded
always_excluded = normalize_path(['src/amalgamation/duckdb.cpp', 'src/amalgamation/duckdb.hpp', 'src/amalgamation/parquet-amalgamation.cpp', 'src/amalgamation/parquet-amalgamation.hpp'])
# files excluded from the amalgamation
excluded_files = ['grammar.cpp', 'grammar.hpp', 'symbols.cpp']
# files excluded from individual file compilation during test_compile
excluded_compilation_files = excluded_files + ['gram.hpp', 'kwlist.hpp', "duckdb-c.cpp"]

linenumbers = False

def get_includes(fpath, text):
    # find all the includes referred to in the directory
    regex_include_statements = re.findall("(^[\t ]*[#][\t ]*include[\t ]+[\"]([^\"]+)[\"])", text, flags=re.MULTILINE)
    include_statements = []
    include_files = []
    # figure out where they are located
    for x in regex_include_statements:
        included_file = x[1]
        if skip_duckdb_includes and 'duckdb' in included_file:
            continue
        if 'extension_helper.cpp' in fpath and included_file.endswith('-extension.hpp'):
            continue
        if x[0] in include_statements:
            raise Exception(f"duplicate include {x[0]} in file {fpath}")
        include_statements.append(x[0])
        included_file = os.sep.join(included_file.split('/'))
        found = False
        for include_path in include_paths:
            ipath = os.path.join(include_path, included_file)
            if os.path.isfile(ipath):
                include_files.append(ipath)
                found = True
                break
        if not found:
            raise Exception('Could not find include file "' + included_file + '", included from file "' + fpath + '"')
    return (include_statements, include_files)

def cleanup_file(text):
    # remove all "#pragma once" notifications
    text = re.sub('#pragma once', '', text)
    return text

# recursively get all includes and write them
written_files = {}

#licenses
licenses = []

def need_to_write_file(current_file, ignore_excluded = False):
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

def find_license(original_file):
    global licenses
    file = original_file
    license = ""
    while True:
        (file, end) = os.path.split(file)
        if file == "":
            break
        potential_license = os.path.join(file, "LICENSE")
        if os.path.exists(potential_license):
            license = potential_license
    if license == "":
        raise "Could not find license for %s" % original_file

    if license not in licenses:
        licenses += [license]

    return licenses.index(license)


def write_file(current_file, ignore_excluded = False):
    global linenumbers
    global written_files
    if not need_to_write_file(current_file, ignore_excluded):
        return ""
    written_files[current_file] = True

    # first read this file
    with open_utf8(current_file, 'r') as f:
        text = f.read()

    if current_file.startswith("third_party") and not current_file.endswith("LICENSE"):
        lic_idx = find_license(current_file)
        text = "\n\n// LICENSE_CHANGE_BEGIN\n// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #%s\n// See the end of this file for a list\n\n" % str(lic_idx + 1) + text + "\n\n// LICENSE_CHANGE_END\n"

    (statements, includes) = get_includes(current_file, text)
    # find the linenr of the final #include statement we parsed
    if len(statements) > 0:
        index = text.find(statements[-1])
        linenr = len(text[:index].split('\n'))

        # now write all the dependencies of this header first
        for i in range(len(includes)):
            include_text = write_file(includes[i])
            if linenumbers and i == len(includes) - 1:
                # for the last include statement, we also include a #line directive
                include_text += '\n#line %d "%s"\n' % (linenr, current_file)
            text = text.replace(statements[i], include_text)

    # add the initial line here
    if linenumbers:
        text = '\n#line 1 "%s"\n' % (current_file,) + text
    # print(current_file)
    # now read the header and write it
    return cleanup_file(text)

def write_dir(dir):
    files = os.listdir(dir)
    files.sort()
    text = ""
    for fname in files:
        if fname in excluded_files:
            continue
        # print(fname)
        fpath = os.path.join(dir, fname)
        if os.path.isdir(fpath):
            text += write_dir(fpath)
        elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
            text += write_file(fpath)
    return text

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

def git_commit_hash():
    return subprocess.check_output(['git','log','-1','--format=%h']).strip().decode('utf8')

def git_dev_version():
    version = subprocess.check_output(['git','describe','--tags','--abbrev=0']).strip().decode('utf8')
    long_version = subprocess.check_output(['git','describe','--tags','--long']).strip().decode('utf8')
    version_splits = version.split('.')
    dev_version = long_version.split('-')[1]
    if int(dev_version) == 0:
        # directly on a tag: emit the regular version
        return '.'.join(version_splits)
    else:
        # not on a tag: increment the version by one and add a -devX suffix
        version_splits[2] = str(int(version_splits[2]) + 1)
        return '.'.join(version_splits) + "-dev" + dev_version

def generate_duckdb_hpp(header_file):
    print("-----------------------")
    print("-- Writing " + header_file + " --")
    print("-----------------------")
    with open_utf8(temp_header, 'w+') as hfile:
        hfile.write("/*\n")
        hfile.write(write_file("LICENSE"))
        hfile.write("*/\n\n")

        hfile.write("#pragma once\n")
        hfile.write("#define DUCKDB_AMALGAMATION 1\n")
        if extended_amalgamation:
            hfile.write("#define DUCKDB_AMALGAMATION_EXTENDED 1\n")
        hfile.write("#define DUCKDB_SOURCE_ID \"%s\"\n" % git_commit_hash())
        hfile.write("#define DUCKDB_VERSION \"%s\"\n" % git_dev_version())

        for fpath in main_header_files:
            hfile.write(write_file(fpath))

def generate_amalgamation(source_file, header_file):
    # construct duckdb.hpp from these headers
    generate_duckdb_hpp(header_file)

    # now construct duckdb.cpp
    print("------------------------")
    print("-- Writing " + source_file + " --")
    print("------------------------")

    # scan all the .cpp files
    with open_utf8(temp_source, 'w+') as sfile:
        header_file_name = header_file.split(os.sep)[-1]
        sfile.write('#include "' + header_file_name + '"\n\n')
        sfile.write("#ifndef DUCKDB_AMALGAMATION\n#error header mismatch\n#endif\n\n")
        sfile.write("#if (!defined(DEBUG) && !defined NDEBUG)\n#define NDEBUG\n#endif\n\n")
        for compile_dir in compile_directories:
            sfile.write(write_dir(compile_dir))

        sfile.write('\n\n/*\n')
        license_idx = 0
        for license in licenses:
            sfile.write("\n\n\n### THIRD PARTY LICENSE #%s ###\n\n" % str(license_idx + 1))
            sfile.write(write_file(license))
            license_idx+=1
        sfile.write('\n\n*/\n')


    copy_if_different(temp_header, header_file)
    copy_if_different(temp_source, source_file)
    try:
        os.remove(temp_header)
        os.remove(temp_source)
    except:
        pass

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
        elif fname.endswith(('.hpp', '.h', '.hh', '.tcc', '.inc')):
            file_list.append(fpath)

def list_includes_files(include_dirs):
    file_list = []
    for include_dir in include_dirs:
        list_include_files_recursive(include_dir, file_list)
    return file_list

def list_includes():
    return list_includes_files(include_paths)

def gather_file(current_file, source_files, header_files):
    global linenumbers
    global written_files
    if not need_to_write_file(current_file, False):
        return ""
    written_files[current_file] = True

    # first read this file
    with open_utf8(current_file, 'r') as f:
        text = f.read()

    (statements, includes) = get_includes(current_file, text)
    # find the linenr of the final #include statement we parsed
    if len(statements) > 0:
        index = text.find(statements[-1])
        linenr = len(text[:index].split('\n'))

        # now write all the dependencies of this header first
        for i in range(len(includes)):
            # source file inclusions are inlined into the main text
            include_text = write_file(includes[i])
            if linenumbers and i == len(includes) - 1:
                # for the last include statement, we also include a #line directive
                include_text += '\n#line %d "%s"\n' % (linenr, current_file)
            if includes[i].endswith('.cpp') or includes[i].endswith('.cc') or includes[i].endswith('.c'):
                # source file inclusions are inlined into the main text
                text = text.replace(statements[i], include_text)
            else:
                text = text.replace(statements[i], '')
                header_files.append(include_text)

    # add the initial line here
    if linenumbers:
        text = '\n#line 1 "%s"\n' % (current_file,) + text
    source_files.append(cleanup_file(text))

def gather_files(dir, source_files, header_files):
    files = os.listdir(dir)
    files.sort()
    for fname in files:
        if fname in excluded_files:
            continue
        fpath = os.path.join(dir, fname)
        if os.path.isdir(fpath):
            gather_files(fpath, source_files, header_files)
        elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
            gather_file(fpath, source_files, header_files)

def write_license(hfile):
    hfile.write("// See https://raw.githubusercontent.com/duckdb/duckdb/master/LICENSE for licensing information\n\n")

def generate_amalgamation_splits(source_file, header_file, nsplits):
    # construct duckdb.hpp from these headers
    generate_duckdb_hpp(header_file)

    # gather all files to read and write
    source_files = []
    header_files = []
    for compile_dir in compile_directories:
        if compile_dir != src_dir:
            continue
        gather_files(compile_dir, source_files, header_files)

    # write duckdb-internal.hpp
    if '.hpp' in header_file:
        internal_header_file = header_file.replace('.hpp', '-internal.hpp')
    elif '.h' in header_file:
        internal_header_file = header_file.replace('.h', '-internal.h')
    else:
        raise "Unknown extension of header file"

    temp_internal_header = internal_header_file + '.tmp'

    with open_utf8(temp_internal_header, 'w+') as f:
        write_license(f)
        for hfile in header_files:
            f.write(hfile)

    # count the total amount of bytes in the source files
    total_bytes = 0
    for sfile in source_files:
        total_bytes += len(sfile)

    # now write the individual splits
    # we approximate the splitting up by making every file have roughly the same amount of bytes
    split_bytes = total_bytes / nsplits
    current_bytes = 0
    partitions = []
    partition_names = []
    current_partition = []
    current_partition_idx = 1
    for sfile in source_files:
        current_partition.append(sfile)
        current_bytes += len(sfile)
        if current_bytes >= split_bytes:
            partition_names.append(str(current_partition_idx))
            partitions.append(current_partition)
            current_partition = []
            current_bytes = 0
            current_partition_idx += 1
    if len(current_partition) > 0:
        partition_names.append(str(current_partition_idx))
        partitions.append(current_partition)
        current_partition = []
        current_bytes = 0
    # generate partitions from the third party libraries
    for compile_dir in compile_directories:
        if compile_dir != src_dir:
            partition_names.append(compile_dir.split(os.sep)[-1])
            partitions.append(write_dir(compile_dir))

    header_file_name = header_file.split(os.sep)[-1]
    internal_header_file_name = internal_header_file.split(os.sep)[-1]

    partition_fnames = []
    current_partition = 0
    for partition in partitions:
        partition_name = source_file.replace('.cpp', '-%s.cpp' % (partition_names[current_partition],))
        temp_partition_name = partition_name + '.tmp'
        partition_fnames.append([partition_name, temp_partition_name])
        with open_utf8(temp_partition_name, 'w+') as f:
            write_license(f)
            f.write('#include "%s"\n#include "%s"' % (header_file_name, internal_header_file_name))
            f.write('''
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif
''')
            for sfile in partition:
                f.write(sfile)
        current_partition += 1

    copy_if_different(temp_header, header_file)
    copy_if_different(temp_internal_header, internal_header_file)
    try:
        os.remove(temp_header)
        os.remove(temp_internal_header)
    except:
        pass
    for p in partition_fnames:
        copy_if_different(p[1], p[0])
        try:
            os.remove(p[1])
        except:
            pass
def list_include_dirs():
    return include_paths

if __name__ == "__main__":
    nsplits = 1
    for arg in sys.argv:
        if arg == '--linenumbers':
            linenumbers = True
        elif arg == '--no-linenumbers':
            linenumbers = False
        elif arg.startswith('--header='):
            header_file = os.path.join(*arg.split('=', 1)[1].split('/'))
        elif arg.startswith('--source='):
            source_file = os.path.join(*arg.split('=', 1)[1].split('/'))
        elif arg.startswith('--splits='):
            nsplits = int(arg.split('=', 1)[1])
        elif arg.startswith('--list-sources'):
            file_list = list_sources()
            print('\n'.join(file_list))
            exit(1)
        elif arg.startswith('--list-objects'):
            file_list = list_sources()
            print(' '.join([x.rsplit('.', 1)[0] + '.o' for x in file_list]))
            exit(1)
        elif arg.startswith('--includes'):
            include_dirs = list_include_dirs()
            print(' '.join(['-I' + x for x in include_dirs]))
            exit(1)
        elif arg.startswith('--include-directories'):
            include_dirs = list_include_dirs()
            print('\n'.join(include_dirs))
            exit(1)
    if not os.path.exists(amal_dir):
        os.makedirs(amal_dir)

    if nsplits > 1:
        generate_amalgamation_splits(source_file, header_file, nsplits)
    else:
        generate_amalgamation(source_file, header_file)

