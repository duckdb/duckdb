#pragma once

#include "duckdb/common/primitive_dictionary.hpp"
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

struct EncodedInput {
	//! The index at which we started encoding the input
	//  in case we switch to FSST_ONLY in the middle, we can avoid encoding the previous input strings
	idx_t offset = 0;
	//! If the append_mode is FSST_ONLY we will encode all input
	//  this memory is owned by a reusable buffer stored in the state
	vector<string_t> data;
};

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct DictFSSTCompressionState : public CompressionState {
public:
	DictFSSTCompressionState(ColumnDataCheckpointData &checkpoint_data_p, unique_ptr<DictFSSTAnalyzeState> &&state);
	~DictFSSTCompressionState() override;

public:
	void CreateEmptySegment();
	idx_t Finalize();

	bool AllUnique() const;
	void FlushEncodingBuffer();
	idx_t CalculateRequiredSpace() const;
	DictionaryAppendState TryEncode();

	bool CompressInternal(UnifiedVectorFormat &vector_format, const string_t &str, bool is_null,
	                      EncodedInput &encoded_input, const idx_t i, idx_t count, bool fail_on_no_space);
	void Compress(Vector &scan_vector, idx_t count);
	void FinalizeCompress();
	void Flush(bool final);

public:
	ColumnDataCheckpointData &checkpoint_data;
	CompressionFunction &function;
	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	//! Offset at which to write the next dictionary string
	idx_t dictionary_offset = 0;

	idx_t string_lengths_space = 0;
	vector<uint32_t> string_lengths;
	uint32_t dict_count = 0;
	bitpacking_width_t string_lengths_width = 0;
	//! For DICT_FSST we delay encoding of new entries, which means
	//  we have to prepare for strings being exploded in size by max 2x
	//  so we have to muddy our 'string_lengths_width'
	bitpacking_width_t real_string_lengths_width = 0;

	idx_t dictionary_indices_space = 0;
	vector<uint32_t> dictionary_indices;
	bitpacking_width_t dictionary_indices_width = 0;

	//! string -> dictionary_index (for lookups)
	PrimitiveDictionary<string_t> current_string_map;
	//! strings added to the dictionary waiting to be encoded
	vector<string_t> dictionary_encoding_buffer;
	idx_t to_encode_string_sum = 0;
	//! for DICT_FSST we store uncompressed strings in the 'current_string_map', this owns that memory
	StringHeap uncompressed_dictionary_copy;

	//! This is used for FSST_ONLY, to store the memory of the encoded input
	unsafe_unique_array<unsigned char> encoding_buffer = nullptr;
	idx_t encoding_buffer_size = 0;

	idx_t tuple_count = 0;
	unique_ptr<DictFSSTAnalyzeState> analyze;
	idx_t symbol_table_size = DConstants::INVALID_INDEX;

	//! How many values have we compressed so far?
	idx_t total_tuple_count = 0;

private:
	void *encoder = nullptr;
	unsafe_unique_array<unsigned char> fsst_serialized_symbol_table;
	DictionaryAppendState append_state = DictionaryAppendState::REGULAR;
};

} // namespace dict_fsst
} // namespace duckdb
