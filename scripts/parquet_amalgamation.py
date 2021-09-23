
import amalgamation
import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'extension', 'parquet'))
import parquet_config
from python_helpers import open_utf8

parquet_amal_base = os.path.join(amalgamation.amal_dir, 'parquet-amalgamation')
parquet_header_file = parquet_amal_base + '.hpp'
parquet_source_file = parquet_amal_base + '.cpp'
temp_header = parquet_header_file + '.tmp'
temp_source = parquet_source_file + '.tmp'
compile_directories = list(set([x.rsplit(os.path.sep, 1)[0] for x in parquet_config.source_files]))

amalgamation.include_paths += parquet_config.include_directories
amalgamation.main_header_files = [
    os.path.sep.join('extension/parquet/include/parquet-extension.hpp'.split('/')),
    os.path.sep.join('extension/parquet/include/parquet_reader.hpp'.split('/')),
    os.path.sep.join('extension/parquet/include/parquet_writer.hpp'.split('/'))]
amalgamation.skip_duckdb_includes = True
amalgamation.always_excluded += ['extension/parquet/parquetcli.cpp']

if not os.path.exists(amalgamation.amal_dir):
    os.makedirs(amalgamation.amal_dir)

def generate_parquet_hpp(header_file):
    print("-----------------------")
    print("-- Writing " + header_file + " --")
    print("-----------------------")
    with open_utf8(temp_header, 'w+') as hfile:
        hfile.write("/*\n")
        hfile.write(amalgamation.write_file("LICENSE"))
        hfile.write("*/\n\n")

        hfile.write("#pragma once\n")
        for fpath in amalgamation.main_header_files:
            hfile.write(amalgamation.write_file(fpath))

def generate_parquet_amalgamation(source_file, header_file):
    # construct duckdb.hpp from these headers
    generate_parquet_hpp(header_file)

    print("------------------------")
    print("-- Writing " + source_file + " --")
    print("------------------------")

    # scan all the .cpp files
    with open_utf8(temp_source, 'w+') as sfile:
        header_file_name = header_file.split(os.sep)[-1]
        sfile.write('''#include "duckdb.hpp"
#ifdef DUCKDB_AMALGAMATION
#ifndef DUCKDB_AMALGAMATION_EXTENDED
#error Parquet amalgamation requires extended DuckDB amalgamation (--extended)
#endif
#endif
''')
        sfile.write('#include "' + header_file_name + '"\n\n')
        for compile_dir in compile_directories:
            sfile.write(amalgamation.write_dir(compile_dir))

    amalgamation.copy_if_different(temp_header, header_file)
    amalgamation.copy_if_different(temp_source, source_file)
    try:
        os.remove(temp_header)
        os.remove(temp_source)
    except:
        pass

generate_parquet_amalgamation(parquet_source_file, parquet_header_file)