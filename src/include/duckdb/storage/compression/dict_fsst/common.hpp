#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"

namespace duckdb {

namespace dict_fsst {

//! This enum holds the available compression modes, this can be expanded later by adding to the end (before COUNT)
enum class DictFSSTMode : uint8_t {
	DICTIONARY = 0,
	DICT_FSST = 1,
	FSST_ONLY = 2,
	COUNT //! Always the last member of the enum
};

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t string_lengths_offset;
	uint32_t string_lengths_width;
	uint32_t dict_count;
	uint32_t dictionary_indices_width;
	DictFSSTMode mode;
} dict_fsst_compression_header_t;

enum class DictionaryAppendState : uint8_t {
	REGULAR,           //! Symbol table threshold not reached yet
	ENCODED,           //! Reached the threshold, decided to encode the dictionary
	NOT_ENCODED,       //! Reached the threshold, decided not to encode the dictionary
	ENCODED_ALL_UNIQUE //! Reached the threshold, decided to encode the dictionary, and all entries so far are unique
};

struct DictFSSTCompression {
public:
	//! Dictionary header size at the beginning of the string segment (offset + length)
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dict_fsst_compression_header_t);
	static constexpr idx_t STRING_SIZE_LIMIT = 16384;

public:
	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
};

} // namespace dict_fsst

} // namespace duckdb
