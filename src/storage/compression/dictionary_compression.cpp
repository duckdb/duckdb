#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

namespace duckdb {

//! Abstract class managing the compression state for size analysis or compression.
class DictionaryCompressionState : public CompressionState {
public:
	explicit DictionaryCompressionState(const CompressionInfo &info) : CompressionState(info) {};

public:
	bool UpdateState(Vector &scan_vector, idx_t count) {
		UnifiedVectorFormat vdata;
		scan_vector.ToUnifiedFormat(count, vdata);
		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		Verify();

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			idx_t string_size = 0;
			bool new_string = false;
			auto row_is_valid = vdata.validity.RowIsValid(idx);

			if (row_is_valid) {
				string_size = data[idx].GetSize();
				if (string_size >= StringUncompressed::GetStringBlockLimit(info.GetBlockSize())) {
					// Big strings not implemented for dictionary compression
					return false;
				}
				new_string = !LookupString(data[idx]);
			}

			bool fits = CalculateSpaceRequirements(new_string, string_size);
			if (!fits) {
				Flush();
				new_string = true;

				fits = CalculateSpaceRequirements(new_string, string_size);
				if (!fits) {
					throw InternalException("Dictionary compression could not write to new segment");
				}
			}

			if (!row_is_valid) {
				AddNull();
			} else if (new_string) {
				AddNewString(data[idx]);
			} else {
				AddLastLookup();
			}

			Verify();
		}

		return true;
	}

protected:
	// Should verify the State
	virtual void Verify() = 0;
	// Performs a lookup of str, storing the result internally
	virtual bool LookupString(string_t str) = 0;
	// Add the most recently looked up str to compression state
	virtual void AddLastLookup() = 0;
	// Add string to the state that is known to not be seen yet
	virtual void AddNewString(string_t str) = 0;
	// Add a null value to the compression state
	virtual void AddNull() = 0;
	// Needs to be called before adding a value. Will return false if a flush is required first.
	virtual bool CalculateSpaceRequirements(bool new_string, idx_t string_size) = 0;
	// Flush the segment to disk if compressing or reset the counters if analyzing
	virtual void Flush(bool final = false) = 0;
};

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t index_buffer_offset;
	uint32_t index_buffer_count;
	uint32_t bitpacking_width;
} dictionary_compression_header_t;

struct DictionaryCompressionStorage {
	static constexpr float MINIMUM_COMPRESSION_RATIO = 1.2F;
	//! Dictionary header size at the beginning of the string segment (offset + length)
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static bool HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width, const idx_t block_size);
	static idx_t RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);

	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, data_ptr_t baseptr,
	                                    int32_t dict_offset, uint16_t string_len);
	static uint16_t GetStringLength(uint32_t *index_buffer_ptr, sel_t index);
};

// Dictionary compression uses a combination of bitpacking and a dictionary to compress string segments. The data is
// stored across three buffers: the index buffer, the selection buffer and the dictionary. Firstly the Index buffer
// contains the offsets into the dictionary which are also used to determine the string lengths. Each value in the
// dictionary gets a single unique index in the index buffer. Secondly, the selection buffer maps the tuples to an index
// in the index buffer. The selection buffer is compressed with bitpacking. Finally, the dictionary contains simply all
// the unique strings without lenghts or null termination as we can deduce the lengths from the index buffer. The
// addition of the selection buffer is done for two reasons: firstly, to allow the scan to emit dictionary vectors by
// scanning the whole dictionary at once and then scanning the selection buffer for each emitted vector. Secondly, it
// allows for efficient bitpacking compression as the selection values should remain relatively small.
struct DictionaryCompressionCompressState : public DictionaryCompressionState {
	DictionaryCompressionCompressState(ColumnDataCheckpointer &checkpointer_p, const CompressionInfo &info)
	    : DictionaryCompressionState(info), checkpointer(checkpointer_p),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_DICTIONARY)),
	      heap(BufferAllocator::Get(checkpointer.GetDatabase())) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	StringHeap heap;
	string_map_t<uint32_t> current_string_map;
	vector<uint32_t> index_buffer;
	vector<uint32_t> selection_buffer;

	bitpacking_width_t current_width = 0;
	bitpacking_width_t next_width = 0;

	// Result of latest LookupString call
	uint32_t latest_lookup_result;

public:
	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();

		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
		current_segment = std::move(compressed_segment);
		current_segment->function = function;

		// Reset the buffers and the string map.
		current_string_map.clear();
		index_buffer.clear();

		// Reserve index 0 for null strings.
		index_buffer.push_back(0);
		selection_buffer.clear();

		current_width = 0;
		next_width = 0;

		// Reset the pointers into the current segment.
		auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = DictionaryCompressionStorage::GetDictionary(*current_segment, current_handle);
		current_end_ptr = current_handle.Ptr() + current_dictionary.end;
	}

	void Verify() override {
		current_dictionary.Verify(info.GetBlockSize());
		D_ASSERT(current_segment->count == selection_buffer.size());
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load(), index_buffer.size(),
		                                                      current_dictionary.size, current_width,
		                                                      info.GetBlockSize()));
		D_ASSERT(current_dictionary.end == info.GetBlockSize());
		D_ASSERT(index_buffer.size() == current_string_map.size() + 1); // +1 is for null value
	}

	bool LookupString(string_t str) override {
		auto search = current_string_map.find(str);
		auto has_result = search != current_string_map.end();

		if (has_result) {
			latest_lookup_result = search->second;
		}
		return has_result;
	}

	void AddNewString(string_t str) override {
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, str);

		// Copy string to dict
		current_dictionary.size += str.GetSize();
		auto dict_pos = current_end_ptr - current_dictionary.size;
		memcpy(dict_pos, str.GetData(), str.GetSize());
		current_dictionary.Verify(info.GetBlockSize());
		D_ASSERT(current_dictionary.end == info.GetBlockSize());

		// Update buffers and map
		index_buffer.push_back(current_dictionary.size);
		selection_buffer.push_back(UnsafeNumericCast<uint32_t>(index_buffer.size() - 1));
		if (str.IsInlined()) {
			current_string_map.insert({str, index_buffer.size() - 1});
		} else {
			current_string_map.insert({heap.AddBlob(str), index_buffer.size() - 1});
		}
		DictionaryCompressionStorage::SetDictionary(*current_segment, current_handle, current_dictionary);

		current_width = next_width;
		current_segment->count++;
	}

	void AddNull() override {
		selection_buffer.push_back(0);
		current_segment->count++;
	}

	void AddLastLookup() override {
		selection_buffer.push_back(latest_lookup_result);
		current_segment->count++;
	}

	bool CalculateSpaceRequirements(bool new_string, idx_t string_size) override {
		if (!new_string) {
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size(),
			                                                    current_dictionary.size, current_width,
			                                                    info.GetBlockSize());
		}
		next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
		return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size() + 1,
		                                                    current_dictionary.size + string_size, next_width,
		                                                    info.GetBlockSize());
	}

	void Flush(bool final = false) override {
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), segment_size);

		if (!final) {
			CreateEmptySegment(next_start);
		}
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == info.GetBlockSize());

		// calculate sizes
		auto compressed_selection_buffer_size =
		    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
		auto index_buffer_size = index_buffer.size() * sizeof(uint32_t);
		auto total_size = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE + compressed_selection_buffer_size +
		                  index_buffer_size + current_dictionary.size;

		// calculate ptr and offsets
		auto base_ptr = handle.Ptr();
		auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(base_ptr);
		auto compressed_selection_buffer_offset = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE;
		auto index_buffer_offset = compressed_selection_buffer_offset + compressed_selection_buffer_size;

		// Write compressed selection buffer
		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_selection_buffer_offset,
		                                               (sel_t *)(selection_buffer.data()), current_segment->count,
		                                               current_width);

		// Write the index buffer
		memcpy(base_ptr + index_buffer_offset, index_buffer.data(), index_buffer_size);

		// Store sizes and offsets in segment header
		Store<uint32_t>(NumericCast<uint32_t>(index_buffer_offset), data_ptr_cast(&header_ptr->index_buffer_offset));
		Store<uint32_t>(NumericCast<uint32_t>(index_buffer.size()), data_ptr_cast(&header_ptr->index_buffer_count));
		Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));

		D_ASSERT(current_width == BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1));
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(
		    current_segment->count, index_buffer.size(), current_dictionary.size, current_width, info.GetBlockSize()));
		D_ASSERT((uint64_t)*max_element(std::begin(selection_buffer), std::end(selection_buffer)) ==
		         index_buffer.size() - 1);

		// Early-out, if the block is sufficiently full.
		if (total_size >= info.GetCompactionFlushLimit()) {
			return info.GetBlockSize();
		}

		// Sufficient space: calculate how much space we can save.
		auto move_amount = info.GetBlockSize() - total_size;

		// Move the dictionary to align it with the offsets.
		auto new_dictionary_offset = index_buffer_offset + index_buffer_size;
		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);

		// Write the new dictionary with the updated "end".
		DictionaryCompressionStorage::SetDictionary(*current_segment, handle, current_dictionary);
		return total_size;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryAnalyzeState : public DictionaryCompressionState {
	explicit DictionaryAnalyzeState(const CompressionInfo &info)
	    : DictionaryCompressionState(info), segment_count(0), current_tuple_count(0), current_unique_count(0),
	      current_dict_size(0), current_width(0), next_width(0) {
	}

	idx_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	idx_t current_dict_size;
	StringHeap heap;
	string_set_t current_set;
	bitpacking_width_t current_width;
	bitpacking_width_t next_width;

	bool LookupString(string_t str) override {
		return current_set.count(str);
	}

	void AddNewString(string_t str) override {
		current_tuple_count++;
		current_unique_count++;
		current_dict_size += str.GetSize();
		if (str.IsInlined()) {
			current_set.insert(str);
		} else {
			current_set.insert(heap.AddBlob(str));
		}
		current_width = next_width;
	}

	void AddLastLookup() override {
		current_tuple_count++;
	}

	void AddNull() override {
		current_tuple_count++;
	}

	bool CalculateSpaceRequirements(bool new_string, idx_t string_size) override {
		if (!new_string) {
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count,
			                                                    current_dict_size, current_width, info.GetBlockSize());
		}
		next_width = BitpackingPrimitives::MinimumBitWidth(current_unique_count + 2); // 1 for null, one for new string
		return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count + 1,
		                                                    current_dict_size + string_size, next_width,
		                                                    info.GetBlockSize());
	}

	void Flush(bool final = false) override {
		segment_count++;
		current_tuple_count = 0;
		current_unique_count = 0;
		current_dict_size = 0;
		current_set.clear();
	}
	void Verify() override {};
};

struct DictionaryCompressionAnalyzeState : public AnalyzeState {
	explicit DictionaryCompressionAnalyzeState(const CompressionInfo &info)
	    : AnalyzeState(info), analyze_state(make_uniq<DictionaryAnalyzeState>(info)) {
	}

	unique_ptr<DictionaryAnalyzeState> analyze_state;
};

unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize(), type);
	return make_uniq<DictionaryCompressionAnalyzeState>(info);
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<DictionaryCompressionAnalyzeState>();
	return state.analyze_state->UpdateState(input, count);
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &analyze_state = state_p.Cast<DictionaryCompressionAnalyzeState>();
	auto &state = *analyze_state.analyze_state;

	auto width = BitpackingPrimitives::MinimumBitWidth(state.current_unique_count + 1);
	auto req_space =
	    RequiredSpace(state.current_tuple_count, state.current_unique_count, state.current_dict_size, width);

	const auto total_space = state.segment_count * state.info.GetBlockSize() + req_space;
	return NumericCast<idx_t>(MINIMUM_COMPRESSION_RATIO * float(total_space));
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                           unique_ptr<AnalyzeState> state) {
	return make_uniq<DictionaryCompressionCompressState>(checkpointer, state->info);
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<DictionaryCompressionCompressState>();
	state.UpdateState(scan_vector, count);
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<DictionaryCompressionCompressState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public StringScanState {
	BufferHandle handle;
	buffer_ptr<Vector> dictionary;
	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
};

unique_ptr<SegmentScanState> DictionaryCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_uniq<CompressedStringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);

	auto baseptr = state->handle.Ptr() + segment.GetBlockOffset();

	// Load header values
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, state->handle);
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	auto index_buffer_count = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_count));
	state->current_width = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width)));

	auto index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);

	state->dictionary = make_buffer<Vector>(segment.type, index_buffer_count);
	auto dict_child_data = FlatVector::GetData<string_t>(*(state->dictionary));

	for (uint32_t i = 0; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = GetStringLength(index_buffer_ptr, i);
		dict_child_data[i] =
		    FetchStringFromDict(segment, dict, baseptr, UnsafeNumericCast<int32_t>(index_buffer_ptr[i]), str_len);
	}

	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, scan_state.handle);

	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	auto index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);

	auto base_data = data_ptr_cast(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	if (!ALLOW_DICT_VECTORS || scan_count != STANDARD_VECTOR_SIZE ||
	    start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
		// Emit regular vector

		// Handling non-bitpacking-group-aligned start values;
		idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

		// Create a decompression buffer of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		data_ptr_t src = &base_data[((start - start_offset) * scan_state.current_width) / 8];
		sel_t *sel_vec_ptr = scan_state.sel_vec->data();

		BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), src, decompress_count,
		                                          scan_state.current_width);

		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = scan_state.sel_vec->get_index(i + start_offset);
			auto dict_offset = index_buffer_ptr[string_number];
			auto str_len = GetStringLength(index_buffer_ptr, UnsafeNumericCast<sel_t>(string_number));
			result_data[result_offset + i] =
			    FetchStringFromDict(segment, dict, baseptr, UnsafeNumericCast<int32_t>(dict_offset), str_len);
		}

	} else {
		D_ASSERT(start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
		D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
		D_ASSERT(result_offset == 0);

		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count);

		// Create a selection vector of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		// Scanning 1024 values, emitting a dict vector
		data_ptr_t dst = data_ptr_cast(scan_state.sel_vec->data());
		data_ptr_t src = data_ptr_cast(&base_data[(start * scan_state.current_width) / 8]);

		BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, scan_count, scan_state.current_width);

		result.Slice(*(scan_state.dictionary), *scan_state.sel_vec, scan_count);
	}
}

void DictionaryCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                              Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                  Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto &handle = state.GetOrInsertHandle(segment);

	auto baseptr = handle.Ptr() + segment.GetBlockOffset();
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, handle);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	auto width = (bitpacking_width_t)Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width));
	auto index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);
	auto base_data = data_ptr_cast(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = NumericCast<idx_t>(row_id) % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// Decompress part of selection buffer we need for this value.
	sel_t decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];
	data_ptr_t src = data_ptr_cast(&base_data[((NumericCast<idx_t>(row_id) - start_offset) * width) / 8]);
	BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(decompression_buffer), src,
	                                          BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE, width);

	auto selection_value = decompression_buffer[start_offset];
	auto dict_offset = index_buffer_ptr[selection_value];
	uint16_t str_len = GetStringLength(index_buffer_ptr, selection_value);

	result_data[result_idx] = FetchStringFromDict(segment, dict, baseptr, NumericCast<int32_t>(dict_offset), str_len);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
bool DictionaryCompressionStorage::HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width, const idx_t block_size) {
	return RequiredSpace(current_count, index_count, dict_size, packing_width) <= block_size;
}

idx_t DictionaryCompressionStorage::RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	idx_t base_space = DICTIONARY_HEADER_SIZE + dict_size;
	idx_t string_number_space = BitpackingPrimitives::GetRequiredSize(current_count, packing_width);
	idx_t index_space = index_count * sizeof(uint32_t);

	idx_t used_space = base_space + index_space + string_number_space;

	return used_space;
}

StringDictionaryContainer DictionaryCompressionStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_size));
	container.end = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_end));
	return container;
}

void DictionaryCompressionStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                                 StringDictionaryContainer container) {
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	Store<uint32_t>(container.size, data_ptr_cast(&header_ptr->dict_size));
	Store<uint32_t>(container.end, data_ptr_cast(&header_ptr->dict_end));
}

string_t DictionaryCompressionStorage::FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict,
                                                           data_ptr_t baseptr, int32_t dict_offset,
                                                           uint16_t string_len) {

	D_ASSERT(dict_offset >= 0 && dict_offset <= NumericCast<int32_t>(segment.GetBlockManager().GetBlockSize()));
	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}

	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	return string_t(str_ptr, string_len);
}

uint16_t DictionaryCompressionStorage::GetStringLength(uint32_t *index_buffer_ptr, sel_t index) {
	if (index == 0) {
		return 0;
	} else {
		return UnsafeNumericCast<uint16_t>(index_buffer_ptr[index] - index_buffer_ptr[index - 1]);
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	return CompressionFunction(
	    CompressionType::COMPRESSION_DICTIONARY, data_type, DictionaryCompressionStorage ::StringInitAnalyze,
	    DictionaryCompressionStorage::StringAnalyze, DictionaryCompressionStorage::StringFinalAnalyze,
	    DictionaryCompressionStorage::InitCompression, DictionaryCompressionStorage::Compress,
	    DictionaryCompressionStorage::FinalizeCompress, DictionaryCompressionStorage::StringInitScan,
	    DictionaryCompressionStorage::StringScan, DictionaryCompressionStorage::StringScanPartial<false>,
	    DictionaryCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DictionaryCompressionFun::TypeIsSupported(const CompressionInfo &info) {
	return info.GetPhysicalType() == PhysicalType::VARCHAR;
}
} // namespace duckdb
