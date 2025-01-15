#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

namespace duckdb {
namespace dict_fsst {

// Dictionary compression uses a combination of bitpacking and a dictionary to compress string segments. The data is
// stored across three buffers: the index buffer, the selection buffer and the dictionary. Firstly the Index buffer
// contains the offsets into the dictionary which are also used to determine the string lengths. Each value in the
// dictionary gets a single unique index in the index buffer. Secondly, the selection buffer maps the tuples to an index
// in the index buffer. The selection buffer is compressed with bitpacking. Finally, the dictionary contains simply all
// the unique strings without lengths or null termination as we can deduce the lengths from the index buffer. The
// addition of the selection buffer is done for two reasons: firstly, to allow the scan to emit dictionary vectors by
// scanning the whole dictionary at once and then scanning the selection buffer for each emitted vector. Secondly, it
// allows for efficient bitpacking compression as the selection values should remain relatively small.

struct EncodedInputData {
public:
	explicit EncodedInputData(Allocator &allocator) : heap(allocator) {
	}

public:
	void Reset() {
		input_data.clear();
		heap.Destroy();
	}

public:
	StringHeap heap;
	vector<string_t> input_data;
};

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct DictFSSTCompressionCompressState : public DictFSSTCompressionState {
public:
	DictFSSTCompressionCompressState(ColumnDataCheckpointData &checkpoint_data_p, const CompressionInfo &info);
	~DictFSSTCompressionCompressState() override;

public:
	void CreateEmptySegment(idx_t row_start);
	void Verify() override;
	optional_idx LookupString(const string_t &str) override;
	void AddNewString(const StringData &str) override;
	void AddNull() override;
	void AddLookup(uint32_t lookup_result) override;
	idx_t RequiredSpace(bool new_string, idx_t string_size) override;
	void Flush(bool final = false) override;
	void EncodeInputStrings(UnifiedVectorFormat &input, idx_t count) override;
	bool EncodeDictionary() override;
	StringData GetString(const string_t *strings, idx_t index, idx_t raw_index) override;
	idx_t Finalize();

public:
	ColumnDataCheckpointData &checkpoint_data;
	CompressionFunction &function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	string_map_t<uint32_t> current_string_map;
	vector<uint32_t> dictionary_string_lengths;
	uint32_t max_length = 0;
	bitpacking_width_t string_length_bitwidth = 0;

	vector<uint32_t> selection_buffer;

	bitpacking_width_t current_width = 0;
	bitpacking_width_t next_width = 0;

	EncodedInputData encoded_input;
};

} // namespace dict_fsst
} // namespace duckdb
