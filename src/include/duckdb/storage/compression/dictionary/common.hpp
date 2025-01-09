#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"

namespace duckdb {
namespace dictionary {

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t index_buffer_offset;
	uint32_t index_buffer_count;
	uint32_t bitpacking_width;
} dictionary_compression_header_t;

struct DictionaryCompression {
public:
	static constexpr float MINIMUM_COMPRESSION_RATIO = 1.2F;
	//! Dictionary header size at the beginning of the string segment (offset + length)
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);

public:
	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
};

} // namespace dictionary
} // namespace duckdb
