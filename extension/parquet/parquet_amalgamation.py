# this script creates a single header + source file combination out of the DuckDB sources
import os, re, sys, shutil, glob


amal_dir = os.path.join('src', 'amalgamation')
header_file = os.path.join(amal_dir, "parquet-extension.hpp")
source_file = os.path.join(amal_dir, "parquet-extension.cpp")
temp_source = 'parquet-extension.cpp.tmp'

include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/parquet/include', 'third_party/parquet', 'third_party/snappy', 'third_party/thrift']]
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/parquet/parquet-extension.cpp', 'third_party/parquet/parquet_constants.cpp',  'third_party/parquet/parquet_types.cpp',  'third_party/thrift/thrift/protocol/TProtocol.cpp',  'third_party/thrift/thrift/transport/TTransportException.cpp',  'third_party/thrift/thrift/transport/TBufferTransports.cpp',  'third_party/snappy/snappy.cc',  'third_party/snappy/snappy-sinksource.cc']]

def generate_amalgamation(source_file, header_file):
    def copy_if_different(src, dest):
        if os.path.isfile(dest):
            # dest exists, check if the files are different
            with open(src, 'r') as f:
                source_text = f.read()
            with open(dest, 'r') as f:
                dest_text = f.read()
            if source_text == dest_text:
                print("Skipping copy of " + src + ", identical copy already exists at " + dest)
                return
        print("Copying " + src + " to " + dest)
        shutil.copyfile(src, dest)


    # the header is unchanged
    copy_if_different('extension/parquet/include/parquet-extension.hpp', header_file)

    # now concat all the source/header files while removing known files

    out = open(temp_source, "w")
    out.write("// Parquet reader amalgamation\n\n#include \"%s\"\n" % os.path.basename(header_file))
    out.close()

    def myglob(path, pattern):
        wd = os.getcwd()
        os.chdir(path)
        files = glob.glob(pattern)
        os.chdir(wd)
        return [f.replace('\\', '/') for f in files]

    headers = ["parquet-extension.hpp"] + myglob("third_party/parquet", "*.h") + myglob("third_party", "thrift/thrift/*.h") + myglob("third_party", "thrift/thrift/**/*.h")  + ['protocol/TCompactProtocol.tcc'] + myglob("third_party/snappy", "*.h") + myglob("third_party/miniz", "*.hpp")

    def rewrite(file_in, file_out):
        # print(file_in)
        a_file = open(file_in, "r")
        out = open(file_out, "a")

        for line in a_file:
            if '#pragma once' in line:
                continue
            found = False
            for header in headers:
                if header in line:
                    found = True
                    break
            if found:
                out.write("// %s" % line)
            else:
                out.write(line)
        out.write("\n")
        out.close()


    # inline all the headers first

    def rewrite_prefix(prefix, files):
        for f in files:
            rewrite("%s/%s" % (prefix, f), temp_source)

    # the local and overall order of these rewrites matters.
    rewrite_prefix('third_party/thrift/thrift', ['transport/PlatformSocket.h','config.h','thrift-config.h','Thrift.h',
        'TLogging.h','transport/TTransportException.h','transport/TTransport.h','protocol/TProtocolException.h',
        'protocol/TProtocol.h','protocol/TVirtualProtocol.h','protocol/TCompactProtocol.h','protocol/TCompactProtocol.tcc',
        'transport/TVirtualTransport.h','transport/TBufferTransports.h','TBase.h','TToString.h', 'protocol/TProtocol.cpp',
        'transport/TTransportException.cpp', 'transport/TBufferTransports.cpp'])

    rewrite_prefix('third_party/parquet', ['windows_compatibility.h', 'parquet_types.h', 'parquet_constants.h',
        'parquet_types.cpp', 'parquet_constants.cpp'])

    rewrite_prefix('third_party/snappy', ['snappy-stubs-public.h', 'snappy.h'])

    rewrite_prefix('third_party/miniz', ['miniz.hpp']) # miniz.cpp is already in duckdb.cpp

    rewrite('third_party/utf8proc/include/utf8proc_wrapper.hpp', temp_source)

    # 'main'
    rewrite('extension/parquet/parquet-extension.cpp', temp_source)

    # snappy last because tons of #defines
    rewrite_prefix('third_party/snappy', ['snappy-stubs-internal.h', 'snappy-internal.h','snappy-sinksource.h',
        'snappy-stubs-internal.cc','snappy-sinksource.cc','snappy.cc'])

    copy_if_different(temp_source, source_file)


if __name__ == "__main__":
    for arg in sys.argv:
        if arg.startswith('--header='):
            header_file = os.path.join(*arg.split('=', 1)[1].split('/'))
        elif arg.startswith('--source='):
            source_file = os.path.join(*arg.split('=', 1)[1].split('/'))

    if not os.path.exists(amal_dir):
        os.makedirs(amal_dir)

    generate_amalgamation(source_file, header_file)
