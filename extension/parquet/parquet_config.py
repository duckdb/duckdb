import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/parquet/include',
        'third_party/parquet',
        'third_party/snappy',
        'third_party/thrift',
        'third_party/zstd/include',
    ]
]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/parquet/parquet_extension.cpp',
        'extension/parquet/column_writer.cpp',
        'extension/parquet/serialize_parquet.cpp',
        'third_party/parquet/parquet_constants.cpp',
        'third_party/parquet/parquet_types.cpp',
        'third_party/thrift/thrift/protocol/TProtocol.cpp',
        'third_party/thrift/thrift/transport/TTransportException.cpp',
        'third_party/thrift/thrift/transport/TBufferTransports.cpp',
        'third_party/snappy/snappy.cc',
        'third_party/snappy/snappy-sinksource.cc',
    ]
]
# zstd
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'third_party/zstd/decompress/zstd_ddict.cpp',
        'third_party/zstd/decompress/huf_decompress.cpp',
        'third_party/zstd/decompress/zstd_decompress.cpp',
        'third_party/zstd/decompress/zstd_decompress_block.cpp',
        'third_party/zstd/common/entropy_common.cpp',
        'third_party/zstd/common/fse_decompress.cpp',
        'third_party/zstd/common/zstd_common.cpp',
        'third_party/zstd/common/error_private.cpp',
        'third_party/zstd/common/xxhash.cpp',
    ]
]
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'third_party/zstd/compress/fse_compress.cpp',
        'third_party/zstd/compress/hist.cpp',
        'third_party/zstd/compress/huf_compress.cpp',
        'third_party/zstd/compress/zstd_compress.cpp',
        'third_party/zstd/compress/zstd_compress_literals.cpp',
        'third_party/zstd/compress/zstd_compress_sequences.cpp',
        'third_party/zstd/compress/zstd_compress_superblock.cpp',
        'third_party/zstd/compress/zstd_double_fast.cpp',
        'third_party/zstd/compress/zstd_fast.cpp',
        'third_party/zstd/compress/zstd_lazy.cpp',
        'third_party/zstd/compress/zstd_ldm.cpp',
        'third_party/zstd/compress/zstd_opt.cpp',
    ]
]
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/parquet/parquet_reader.cpp',
        'extension/parquet/parquet_timestamp.cpp',
        'extension/parquet/parquet_writer.cpp',
        'extension/parquet/column_reader.cpp',
        'extension/parquet/parquet_statistics.cpp',
        'extension/parquet/parquet_metadata.cpp',
        'extension/parquet/zstd_file_system.cpp',
    ]
]
