#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/fsst.hpp"
#include "fsst.h"

namespace duckdb {

string_t FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, Vector &result, const char *compressed_string,
                                         const idx_t compressed_string_len, const idx_t block_size) {
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto fsst_decoder = reinterpret_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);

	auto string_block_limit = StringUncompressed::GetStringBlockLimit(block_size);
	vector<unsigned char> decompress_buffer;
	decompress_buffer.resize(string_block_limit + 1);

	auto compressed_string_ptr = (unsigned char *)compressed_string; // NOLINT
	auto decompressed_string_size = duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
	                                                       string_block_limit + 1, decompress_buffer.data());
	D_ASSERT(decompressed_string_size <= string_block_limit);

	return StringVector::AddStringOrBlob(result, const_char_ptr_cast(decompress_buffer.data()),
	                                     decompressed_string_size);
}

Value FSSTPrimitives::DecompressValue(void *duckdb_fsst_decoder, const char *compressed_string,
                                      const idx_t compressed_string_len, const idx_t block_size) {
	auto string_block_limit = StringUncompressed::GetStringBlockLimit(block_size);
	vector<unsigned char> decompress_buffer;
	decompress_buffer.resize(string_block_limit + 1);

	auto compressed_string_ptr = (unsigned char *)compressed_string; // NOLINT
	auto fsst_decoder = reinterpret_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
	auto decompressed_string_size = duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
	                                                       string_block_limit + 1, decompress_buffer.data());
	D_ASSERT(decompressed_string_size <= string_block_limit);

	return Value(string(char_ptr_cast(decompress_buffer.data()), decompressed_string_size));
}

} // namespace duckdb
