#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"

namespace duckdb {

namespace dict_fsst {

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t index_buffer_offset;
	uint32_t index_buffer_count;
	uint32_t bitpacking_width;
	bool fsst_encoded;
} dict_fsst_compression_header_t;

struct DictFSSTCompression {
public:
	static constexpr float MINIMUM_COMPRESSION_RATIO = 1.2F;
	//! Dictionary header size at the beginning of the string segment (offset + length)
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dict_fsst_compression_header_t);

public:
	static bool HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width, const idx_t block_size);
	static idx_t RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);

	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
};

//! Abstract class managing the compression state for size analysis or compression.
class DictFSSTCompressionState : public CompressionState {
public:
	explicit DictFSSTCompressionState(const CompressionInfo &info);
	~DictFSSTCompressionState() override;

public:
	bool UpdateState(Vector &scan_vector, idx_t count);

protected:
	// Should verify the State
	virtual void Verify() = 0;
	// Performs a lookup of str, storing the result internally
	virtual optional_idx LookupString(string_t str) = 0;
	// Add the most recently looked up str to compression state
	virtual void AddLookup(uint32_t lookup_result) = 0;
	// Add string to the state that is known to not be seen yet
	virtual void AddNewString(string_t str) = 0;
	// Add a null value to the compression state
	virtual void AddNull() = 0;
	// Needs to be called before adding a value. Will return false if a flush is required first.
	virtual bool HasRoomForString(bool new_string, idx_t string_size) = 0;
	// Flush the segment to disk if compressing or reset the counters if analyzing
	virtual void Flush(bool final = false) = 0;

protected:
	//! Whether the dictionary has been encoded with FSST
	bool fsst_encoded = false;
};

} // namespace dict_fsst

} // namespace duckdb
