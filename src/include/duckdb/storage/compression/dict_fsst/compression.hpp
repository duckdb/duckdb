#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/storage/compression/dict_fsst/analyze.hpp"
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

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct DictFSSTCompressionCompressState : public CompressionState {
public:
	DictFSSTCompressionCompressState(ColumnDataCheckpointData &checkpoint_data_p,
	                                 unique_ptr<DictFSSTAnalyzeState> &&state);
	~DictFSSTCompressionCompressState() override;

public:
	void CreateEmptySegment(idx_t row_start);
	idx_t Finalize();

	void Compress(Vector &scan_vector, idx_t count);
	void FinalizeCompress();
	void Flush();

public:
	ColumnDataCheckpointData &checkpoint_data;
	CompressionFunction &function;
	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	StringDictionaryContainer current_dictionary;
	//! Offset at which to write the next dictionary string
	idx_t dictionary_offset = 0;

public:
	idx_t string_lengths_space;
	vector<uint32_t> string_lengths;
	idx_t dict_count = 0;
	bitpacking_width_t string_lengths_width = 0;
	//! For DICT_FSST we delay encoding of new entries, which means
	//  we have to prepare for strings being exploded in size by max 2x
	//  so we have to muddy out 'string_lengths_width'
	bitpacking_width_t real_string_lengths_width = 0;
	uint32_t max_string_length = 0;

	idx_t dictionary_indices_space;
	vector<uint32_t> dictionary_indices;
	bitpacking_width_t dictionary_indices_width = 0;
	//! uint32_t max_dictionary_index; (this is 'dict_count')

	//! string -> dictionary_index (for lookups)
	string_map_t<uint32_t> current_string_map;
	//! strings added to the dictionary waiting to be encoded
	vector<string_t> dictionary_encoding_buffer;
	//! for DICT_FSST we store uncompressed strings in the 'current_string_map', this owns that memory
	StringHeap uncompressed_dictionary_copy;

	//! This is used for FSST_ONLY, to store the memory of the encoded input
	unsafe_unique_array<unsigned char> encoding_buffer;
	idx_t encoding_buffer_size = 0;

public:
	void *encoder = nullptr;
	idx_t symbol_table_size = DConstants::INVALID_INDEX;
	DictionaryAppendState append_state = DictionaryAppendState::REGULAR;
	bool all_unique = true;

public:
	idx_t tuple_count = 0;
	unique_ptr<DictFSSTAnalyzeState> analyze;
};

} // namespace dict_fsst
} // namespace duckdb
