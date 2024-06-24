#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/fsst.hpp"
#include "fsst.h"

namespace duckdb {

string_t FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, Vector &result, const char *compressed_string,
                                         const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer) {

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto fsst_decoder = reinterpret_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
	auto compressed_string_ptr = (unsigned char *)compressed_string; // NOLINT
	auto decompressed_string_size = duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
	                                                       decompress_buffer.size(), decompress_buffer.data());

	D_ASSERT(!decompress_buffer.empty());
	D_ASSERT(decompressed_string_size <= decompress_buffer.size() - 1);
	return StringVector::AddStringOrBlob(result, const_char_ptr_cast(decompress_buffer.data()),
	                                     decompressed_string_size);
}

Value FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, const char *compressed_string,
                                      const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer) {

	auto compressed_string_ptr = (unsigned char *)compressed_string; // NOLINT
	auto fsst_decoder = reinterpret_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
	auto decompressed_string_size = duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
	                                                       decompress_buffer.size(), decompress_buffer.data());

	D_ASSERT(!decompress_buffer.empty());
	D_ASSERT(decompressed_string_size <= decompress_buffer.size() - 1);
	return Value(string(char_ptr_cast(decompress_buffer.data()), decompressed_string_size));
}

} // namespace duckdb
