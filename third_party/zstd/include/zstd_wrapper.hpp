//===----------------------------------------------------------------------===//
//                         DuckDB
//
// zstd_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "zstd.h"
#include <string>
#include <stdexcept>

namespace duckdb {

struct ZSTDWrapper {
public:
	ZSTDWrapper()  {
	}
	~ZSTDWrapper() {
	}
	void Decompress(const char *compressed_data, size_t compressed_size, char *out_data, size_t out_size) {
		duckdb_zstd::ZSTD_decompress(out_data, out_size, compressed_data, compressed_size);
	}
	size_t MaxCompressedLength(size_t input_size) {
		return duckdb_zstd::ZSTD_compressBound(input_size);
	}
	void Compress(const char *uncompressed_data, size_t uncompressed_size, char *out_data, size_t *out_size) {
		*out_size = duckdb_zstd::ZSTD_compress(out_data, *out_size, uncompressed_data, uncompressed_size, 3);
	}
};

}
