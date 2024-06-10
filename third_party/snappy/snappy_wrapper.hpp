#pragma once

#include <dlfcn.h>

namespace duckdb {

class SnappyWrapper {
public:
	size_t MaxCompressedLength(size_t source_bytes);
	void RawCompress(const char *input, size_t input_length, char *compressed, size_t *compressed_length);
	bool GetUncompressedLength(const char *compressed, size_t compressed_length, size_t *result);
	bool RawUncompress(const char *compressed, size_t compressed_length, char *uncompressed);

private:
	static void *handle;
};

} // namespace duckdb
