#include "snappy_wrapper.hpp"

#include "duckdb/common/printer.hpp"
#include "snappy.h"

namespace duckdb {

void *SnappyWrapper::handle = dlopen("./libsnappy.dylib", RTLD_LAZY);

size_t SnappyWrapper::MaxCompressedLength(size_t source_bytes) {
	typedef size_t (*max_uncompressed_length_t)(size_t);
	if (handle) {
		auto fun = (max_uncompressed_length_t)dlsym(handle, "MaxCompressedLength");
		const char *dlsym_error = dlerror();
		if (!dlsym_error) {
			Printer::Print("hi");
			return fun(source_bytes);
		}
	}
	return duckdb_snappy::MaxCompressedLength(source_bytes);
}

void SnappyWrapper::RawCompress(const char *input, size_t input_length, char *compressed, size_t *compressed_length) {
	typedef void (*raw_compress_t)(const char *, size_t, char *, size_t *);
	if (handle) {

	} else {
		duckdb_snappy::RawCompress(input, input_length, compressed, compressed_length);
	}
}

bool SnappyWrapper::GetUncompressedLength(const char *compressed, size_t compressed_length, size_t *result) {
	typedef bool (*get_uncompressed_length_t)(const char *, size_t, size_t *);
	if (handle) {

	} else {
		return duckdb_snappy::GetUncompressedLength(compressed, compressed_length, result);
	}
}

bool SnappyWrapper::RawUncompress(const char *compressed, size_t compressed_length, char *uncompressed) {
	typedef bool (*raw_uncompress_t)(const char *, size_t, char *);
	if (handle) {

	} else {
		return duckdb_snappy::RawUncompress(compressed, compressed_length, uncompressed);
	}
}

} // namespace duckdb