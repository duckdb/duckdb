#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/fsst.hpp"
#include "fsst.h"

namespace duckdb {
string_t FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, Vector &result, unsigned char *compressed_string,
                                         idx_t compressed_string_len) {
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	unsigned char decompress_buffer[StringUncompressed::STRING_BLOCK_LIMIT + 1];
	auto decompressed_string_size =
	    duckdb_fsst_decompress((duckdb_fsst_decoder_t *)duckdb_fsst_decoder, compressed_string_len, compressed_string,
	                           StringUncompressed::STRING_BLOCK_LIMIT + 1, &decompress_buffer[0]);
	D_ASSERT(decompressed_string_size <= StringUncompressed::STRING_BLOCK_LIMIT);

	return StringVector::AddStringOrBlob(result, (const char *)decompress_buffer, decompressed_string_size);
}

Value FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, unsigned char *compressed_string,
                                      idx_t compressed_string_len) {
	unsigned char decompress_buffer[StringUncompressed::STRING_BLOCK_LIMIT + 1];
	auto decompressed_string_size =
	    duckdb_fsst_decompress((duckdb_fsst_decoder_t *)duckdb_fsst_decoder, compressed_string_len, compressed_string,
	                           StringUncompressed::STRING_BLOCK_LIMIT + 1, &decompress_buffer[0]);
	D_ASSERT(decompressed_string_size <= StringUncompressed::STRING_BLOCK_LIMIT);

	return Value(string((char *)decompress_buffer, decompressed_string_size));
}

} // namespace duckdb
