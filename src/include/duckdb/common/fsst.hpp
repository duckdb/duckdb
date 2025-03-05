//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/fsst.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/fsst.hpp"
#include "fsst.h"
#include "duckdb/common/types/vector_buffer.hpp"

namespace duckdb {

class Value;
class Vector;
struct string_t;

class FSSTPrimitives {
private:
	// This allows us to decode FSST strings efficiently directly into a string_t (if inlined)
	// The decode will overflow a bit into "extra_space", but "str" will contain the full string
	struct StringWithExtraSpace {
		string_t str;
		uint64_t extra_space[string_t::INLINE_LENGTH];
	};

public:
	static string_t DecompressValue(void *duckdb_fsst_decoder, VectorStringBuffer &str_buffer,
	                                const char *compressed_string, const idx_t compressed_string_len) {
		const auto max_uncompressed_length = compressed_string_len * 8;
		const auto fsst_decoder = static_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
		const auto compressed_string_ptr = (const unsigned char *)compressed_string; // NOLINT
		const auto target_ptr = str_buffer.AllocateShrinkableBuffer(max_uncompressed_length);
		const auto decompressed_string_size = duckdb_fsst_decompress(
		    fsst_decoder, compressed_string_len, compressed_string_ptr, max_uncompressed_length, target_ptr);
		return str_buffer.FinalizeShrinkableBuffer(target_ptr, max_uncompressed_length, decompressed_string_size);
	}
	static string_t DecompressInlinedValue(void *duckdb_fsst_decoder, const char *compressed_string,
	                                       const idx_t compressed_string_len) {
		const auto fsst_decoder = static_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
		const auto compressed_string_ptr = (const unsigned char *)compressed_string; // NOLINT
		StringWithExtraSpace result;
		const auto target_ptr = (unsigned char *)result.str.GetPrefixWriteable(); // NOLINT
		const auto decompressed_string_size =
		    duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
		                           string_t::INLINE_LENGTH + sizeof(StringWithExtraSpace::extra_space), target_ptr);
		if (decompressed_string_size > string_t::INLINE_LENGTH) {
			throw IOException("Corrupt database file: decoded FSST string of >=%llu bytes (should be <=%llu bytes)",
			                  decompressed_string_size, string_t::INLINE_LENGTH);
		}
		D_ASSERT(decompressed_string_size <= string_t::INLINE_LENGTH);
		result.str.SetSizeAndFinalize(UnsafeNumericCast<uint32_t>(decompressed_string_size));
		return result.str;
	}
	static string DecompressValue(void *duckdb_fsst_decoder, const char *compressed_string,
	                              const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer);
};
} // namespace duckdb
