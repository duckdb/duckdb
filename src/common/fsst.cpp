#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/fsst.hpp"
#include "fsst.h"

namespace duckdb {

string_t FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, Vector &result, const char *compressed_string,
                                         idx_t compressed_string_len) {
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	unsigned char decompress_buffer[StringUncompressed::STRING_BLOCK_LIMIT + 1];
	auto fsst_decoder = reinterpret_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
	auto compressed_string_ptr = (unsigned char *)compressed_string; // NOLINT
	auto decompressed_string_size =
	    duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
	                           StringUncompressed::STRING_BLOCK_LIMIT + 1, &decompress_buffer[0]);
	D_ASSERT(decompressed_string_size <= StringUncompressed::STRING_BLOCK_LIMIT);

	return StringVector::AddStringOrBlob(result, const_char_ptr_cast(decompress_buffer), decompressed_string_size);
}

Value FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, const char *compressed_string,
                                      idx_t compressed_string_len) {
	unsigned char decompress_buffer[StringUncompressed::STRING_BLOCK_LIMIT + 1];
	auto compressed_string_ptr = (unsigned char *)compressed_string; // NOLINT
	auto fsst_decoder = reinterpret_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
	auto decompressed_string_size =
	    duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
	                           StringUncompressed::STRING_BLOCK_LIMIT + 1, &decompress_buffer[0]);
	D_ASSERT(decompressed_string_size <= StringUncompressed::STRING_BLOCK_LIMIT);

	return Value(string(char_ptr_cast(decompress_buffer), decompressed_string_size));
}

} // namespace duckdb
