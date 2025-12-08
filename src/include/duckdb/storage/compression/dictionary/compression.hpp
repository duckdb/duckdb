#pragma once

#include "duckdb/common/primitive_dictionary.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/compression/dictionary/common.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

namespace duckdb {

// Dictionary compression uses a combination of bitpacking and a dictionary to compress string segments. The data is
// stored across three buffers: the index buffer, the selection buffer and the dictionary. Firstly the Index buffer
// contains the offsets into the dictionary which are also used to determine the string lengths. Each value in the
// dictionary gets a single unique index in the index buffer. Secondly, the selection buffer maps the tuples to an index
// in the index buffer. The selection buffer is compressed with bitpacking. Finally, the dictionary contains simply all
// the unique strings without lengths or null termination as we can deduce the lengths from the index buffer. The
// addition of the selection buffer is done for two reasons: firstly, to allow the scan to emit dictionary vectors by
// scanning the whole dictionary at once and then scanning the selection buffer for each emitted vector. Secondly, it
// allows for efficient bitpacking compression as the selection values should remain relatively small.

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct DictionaryCompressionCompressState : public DictionaryCompressionState {
public:
	DictionaryCompressionCompressState(ColumnDataCheckpointData &checkpoint_data_p, const CompressionInfo &info,
	                                   idx_t max_unique_count_across_all_segments);

public:
	void CreateEmptySegment();
	void Verify() override;
	bool LookupString(string_t str) override;
	void AddNewString(string_t str) override;
	void AddNull() override;
	void AddLastLookup() override;
	bool CalculateSpaceRequirements(bool new_string, idx_t string_size) override;
	void Flush(bool final = false) override;
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
	PrimitiveDictionary<string_t> current_string_map;
	vector<uint32_t> index_buffer;
	vector<uint32_t> selection_buffer;

	bitpacking_width_t current_width = 0;
	bitpacking_width_t next_width = 0;

	// Result of latest LookupString call
	uint32_t latest_lookup_result;
};

} // namespace duckdb
