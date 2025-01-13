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

enum class DictionaryAppendState : uint8_t {
	REGULAR,    //! Symbol table threshold not reached yet
	ENCODED,    //! Reached the threshold, decided to encode the dictionary
	NOT_ENCODED //! Reached the threshold, decided not to encode the dictionary
};

struct DictFSSTCompression {
public:
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

struct StringData {
public:
	explicit StringData(const string_t &string, optional_ptr<const string_t> encoded_string = nullptr)
	    : string(string), encoded_string(encoded_string) {
	}

public:
	const string_t &Get() const {
		if (encoded_string) {
			return *encoded_string;
		}
		return string;
	}

public:
	const string_t &string;
	optional_ptr<const string_t> encoded_string;
};

//! Abstract class managing the compression state for size analysis or compression.
class DictFSSTCompressionState : public CompressionState {
public:
	static constexpr idx_t DICTIONARY_ENCODE_THRESHOLD = 4096;

public:
	explicit DictFSSTCompressionState(const CompressionInfo &info);
	~DictFSSTCompressionState() override;

public:
	bool UpdateState(Vector &scan_vector, idx_t count);

protected:
	// Should verify the State
	virtual void Verify() = 0;
	// Performs a lookup of str, storing the result internally
	virtual optional_idx LookupString(const string_t &str) = 0;
	// Add the most recently looked up str to compression state
	virtual void AddLookup(uint32_t lookup_result) = 0;
	// Add string to the state that is known to not be seen yet
	virtual void AddNewString(const StringData &str) = 0;
	// Add a null value to the compression state
	virtual void AddNull() = 0;
	virtual idx_t RequiredSpace(bool new_string, idx_t string_size) = 0;
	// Flush the segment to disk if compressing or reset the counters if analyzing
	virtual void Flush(bool final = false) = 0;
	virtual void UpdateStats(UnifiedVectorFormat &input, idx_t count) {/* no-op */};
	// Process the strings of the vector if necessary
	virtual void EncodeInputStrings(UnifiedVectorFormat &input, idx_t count) = 0;
	// Encode the dictionary with FSST, return false if we decided not to encode
	virtual bool EncodeDictionary() = 0;
	// Retrieve the string given the indices
	virtual StringData GetString(const string_t *strings, idx_t index, idx_t raw_index) = 0;

private:
	bool DryAppendToCurrentSegment(bool is_new, UnifiedVectorFormat &vdata, idx_t count, idx_t index, idx_t raw_index);

public:
	//! Keep track of the append state for the current segment
	DictionaryAppendState append_state = DictionaryAppendState::REGULAR;
	void *encoder = nullptr;
	idx_t symbol_table_size = DConstants::INVALID_INDEX;
};

} // namespace dict_fsst

} // namespace duckdb
