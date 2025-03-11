import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/parquet/include',
        'third_party/parquet',
        'third_party/thrift',
        'third_party/lz4',
        'third_party/brotli/include',
        'third_party/brotli/common',
        'third_party/brotli/dec',
        'third_party/brotli/enc',
        'third_party/snappy',
        'third_party/mbedtls',
        'third_party/mbedtls/include',
        'third_party/zstd/include',
    ]
]
prefix = os.path.join('extension', 'parquet')


def list_files_recursive(rootdir, suffix):
    file_list = []
    for root, _, files in os.walk(rootdir):
        file_list += [os.path.join(root, f) for f in files if f.endswith(suffix)]
    return file_list


source_files = list_files_recursive(prefix, '.cpp')

# parquet/thrift/snappy
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'third_party/parquet/parquet_types.cpp',
        'third_party/thrift/thrift/protocol/TProtocol.cpp',
        'third_party/thrift/thrift/transport/TTransportException.cpp',
        'third_party/thrift/thrift/transport/TBufferTransports.cpp',
        'third_party/snappy/snappy.cc',
        'third_party/snappy/snappy-sinksource.cc',
    ]
]
# lz4
source_files += [os.path.sep.join(x.split('/')) for x in ['third_party/lz4/lz4.cpp']]

# brotli
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'third_party/brotli/common/constants.cpp',
        'third_party/brotli/common/context.cpp',
        'third_party/brotli/common/dictionary.cpp',
        'third_party/brotli/common/platform.cpp',
        'third_party/brotli/common/shared_dictionary.cpp',
        'third_party/brotli/common/transform.cpp',
        'third_party/brotli/dec/bit_reader.cpp',
        'third_party/brotli/dec/decode.cpp',
        'third_party/brotli/dec/huffman.cpp',
        'third_party/brotli/dec/state.cpp',
        'third_party/brotli/enc/backward_references.cpp',
        'third_party/brotli/enc/backward_references_hq.cpp',
        'third_party/brotli/enc/bit_cost.cpp',
        'third_party/brotli/enc/block_splitter.cpp',
        'third_party/brotli/enc/brotli_bit_stream.cpp',
        'third_party/brotli/enc/cluster.cpp',
        'third_party/brotli/enc/command.cpp',
        'third_party/brotli/enc/compound_dictionary.cpp',
        'third_party/brotli/enc/compress_fragment.cpp',
        'third_party/brotli/enc/compress_fragment_two_pass.cpp',
        'third_party/brotli/enc/dictionary_hash.cpp',
        'third_party/brotli/enc/encode.cpp',
        'third_party/brotli/enc/encoder_dict.cpp',
        'third_party/brotli/enc/entropy_encode.cpp',
        'third_party/brotli/enc/fast_log.cpp',
        'third_party/brotli/enc/histogram.cpp',
        'third_party/brotli/enc/literal_cost.cpp',
        'third_party/brotli/enc/memory.cpp',
        'third_party/brotli/enc/metablock.cpp',
        'third_party/brotli/enc/static_dict.cpp',
        'third_party/brotli/enc/utf8_util.cpp',
    ]
]
