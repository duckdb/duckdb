#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

#include <duckdb/main/config.hpp>
#include <duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp>
#include <duckdb/storage/table/append_state.hpp>

namespace duckdb {

// Abstract class for keeping compression state either for compression or size analysis
class DictionaryCompressionState : public CompressionState {
public:
	void UpdateState(Vector &scan_vector, idx_t count) {
		VectorData vdata;
		scan_vector.Orrify(count, vdata);
		auto data = (string_t *)vdata.data;
		Verify();

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			size_t string_size = 0;
			bool new_string = false;
			auto row_is_valid = vdata.validity.RowIsValid(idx);

			if (row_is_valid) {
				string_size = data[idx].GetSize();
				if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
					throw NotImplementedException("Big strings not implemented for dictionary compression");
				}
				new_string = !LookupString(data[idx].GetString());
			}

			bool fits = HasEnoughSpace(new_string, string_size);
			if (!fits){
				Flush();
				new_string = true;
				D_ASSERT(HasEnoughSpace(new_string, string_size));
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
	}

protected:
	// Should verify the State
	virtual void Verify() = 0;
	// Performs a lookup of str, storing the result internally
	virtual bool LookupString(string str) = 0;
	// Add the most recently looked up str to compression state
	virtual void AddLastLookup() = 0;
	// Add string to the state that is known to not be seen yet
	virtual void AddNewString(string_t str) = 0;
	// Add a null value to the compression state
	virtual void AddNull() = 0;
	// Check if we have enough space to add a string
	virtual bool HasEnoughSpace(bool new_string, size_t string_size) = 0;
	// Flush the segment to disk if compressing or reset the counters if analyzing
	virtual void Flush() = 0;
};

struct DictionaryCompressionCompressState;
struct DictionaryCompressionStorage : UncompressedStringStorage { // TODO REFACTOR? is this inheritance sensible still?
	// HEADER: dict_end, dict_size, buffer_index_start, buffer_index_count, bitpacking_width;
	// TODO: make a struct for this
	static constexpr uint16_t DICTIONARY_HEADER_SIZE =
	    sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static bool HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);
	static idx_t RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);

	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
	                                    data_ptr_t baseptr, int32_t dict_offset, uint16_t string_len);

	static uint16_t GetStringLength(int32_t *index_buffer_ptr, int32_t string_number);
};

// Dictionary compression uses a combination of bitpacking and a dictionary to compress string segments. The data is stored
// across three buffers: the index buffer, the selection buffer and the dictionary. Firstly the Index buffer contains the offsets into the
// dictionary which are also used to determine the string lengths. Each value in the dictionary gets a single unique index in the index
// buffer. Secondly, the selection buffer maps the tuples to an index in the index buffer. The selection buffer is compressed with bitpacking.
// Finally, the dictionary contains simply all the unique strings without lenghts or null termination as we can deduce the lengths from the index buffer.
// The addition of the selection buffer is done for two reasons: firstly, to allow the scan to emit dictionary vectors by scanning the whole dictionary
// at once and then scanning the selection buffer for each emitted vector. Secondly, it allows for efficient bitpacking compression as
// the selection values should remain relatively small.
struct DictionaryCompressionCompressState : public DictionaryCompressionState {
	explicit DictionaryCompressionCompressState(ColumnDataCheckpointer &checkpointer) : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_DICTIONARY, PhysicalType::VARCHAR);
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = move(compressed_segment);

		current_segment->function = function;

		// Reset the buffers and string map
		current_string_map = make_unique<std::unordered_map<string, int32_t>>();
		index_buffer = make_unique<std::vector<int32_t>>();
		index_buffer->push_back(0); // Reserve index 0 for null strings
		selection_buffer = make_unique<std::vector<int32_t>>();

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = UncompressedStringStorage::GetDictionary(*current_segment, *current_handle);
		current_end_ptr = current_handle->node->buffer + current_segment->GetBlockOffset() + current_dictionary.end;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> current_handle; // Do we really need the handle?
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	unique_ptr<std::unordered_map<string, int32_t>> current_string_map;
	unique_ptr<std::vector<int32_t>> index_buffer;
	unique_ptr<std::vector<int32_t>> selection_buffer;

	// The minimal bitpacking width that can be used for the selection buffer
	bitpacking_width_t min_width = sizeof(sel_t) * 8;

	// Result of latest LookupString call
	int32_t latest_lookup_result;

	void Verify() override {
		current_dictionary.Verify();
		D_ASSERT(current_segment->count == selection_buffer->size());
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load(), index_buffer->size(), current_dictionary.size, min_width));
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);
		D_ASSERT(index_buffer->size() == current_string_map->size() + 1); // +1 is for null value
	}

	bool LookupString(string str) override {
		auto search = current_string_map->find(str);
		auto has_result = search != current_string_map->end();

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
		memcpy(dict_pos, str.GetDataUnsafe(), str.GetSize());
		current_dictionary.Verify();
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// Update buffers and map
		index_buffer->push_back(current_dictionary.size);
		selection_buffer->push_back(index_buffer->size() - 1);
		current_string_map->insert({str.GetString(), index_buffer->size() - 1});
		UncompressedStringStorage::SetDictionary(*current_segment, *current_handle, current_dictionary);

		min_width = BitpackingPrimitives::MinimumBitWidth(index_buffer->size() - 1); // TODO this value has been calculated already
		current_segment->count++;
	}

	void AddNull() override {
		selection_buffer->push_back(0);
		current_segment->count++;
	}

	void AddLastLookup() override {
		selection_buffer->push_back(latest_lookup_result);
		current_segment->count++;
	}

	// TODO deduplicate?
	bool HasEnoughSpace(bool new_string, size_t string_size) override {
		auto new_width = BitpackingPrimitives::MinimumBitWidth(index_buffer->size() + new_string);
		if (new_string){
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load()+1, index_buffer->size()+1, current_dictionary.size + string_size, new_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load()+1, index_buffer->size(), current_dictionary.size, new_width);
		}
	}

	void Flush() override {
		min_width = BitpackingPrimitives::MinimumBitWidth(index_buffer->size()-1);
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(move(current_segment), segment_size);

		// Todo this is not necessary if this is called from finalize?
		CreateEmptySegment(next_start);
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// compute the total size required to store this segment
		auto index_buffer_size = index_buffer->size() * sizeof(int32_t); // TODO: is this really the required size?
		auto offset_size =
		    DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE + BitpackingPrimitives::GetRequiredSize<sel_t>(current_segment->count, min_width);
		auto total_size = offset_size + current_dictionary.size + index_buffer_size;

		auto baseptr = handle->node->buffer + current_segment->GetBlockOffset();

		// Write the index buffer next to the index buffer offsets and store its offset in the header
		memcpy(baseptr + offset_size, index_buffer->data(), index_buffer_size);
		Store<uint32_t>(offset_size, baseptr + 2 * sizeof(uint32_t)); // TODO segment offset?
		Store<uint32_t>(index_buffer->size(), baseptr + 3 * sizeof(uint32_t)); // TODO segment offset?
		Store<uint32_t>((uint32_t)min_width, baseptr + 4 * sizeof(uint32_t));

		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count, index_buffer->size(), current_dictionary.size, min_width));
		D_ASSERT((uint64_t)*max_element(std::begin(*selection_buffer), std::end(*selection_buffer)) < index_buffer->size());

		data_ptr_t dst = baseptr + DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE;
		sel_t *src = (sel_t *)(selection_buffer->data());

		// Note: PackBuffer may write beyond dst+count*width, however BitpackingPrimitives::GetRequiredSize, has made sure
		// we have enough space in dst
		BitpackingPrimitives::PackBuffer<sel_t, false>(dst, src, current_segment->count, min_width);

		// TODO: deduplicate with uncompressed_string?
		if (total_size >= Storage::BLOCK_SIZE / 5 * 4) {
			// the block is full enough, don't bother moving around the dictionary
			return Storage::BLOCK_SIZE;
		}
		// the block has space left: figure out how much space we can save
		auto move_amount = Storage::BLOCK_SIZE - total_size;
		// move the dictionary so it lines up exactly with the offsets
		memmove(baseptr + offset_size + index_buffer_size, baseptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);
		// write the new dictionary (with the updated "end")
		DictionaryCompressionStorage::SetDictionary(*current_segment, *handle, current_dictionary);
		return total_size;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryCompressionAnalyzeState : public AnalyzeState, DictionaryCompressionState {
	DictionaryCompressionAnalyzeState() : segment_count(0), current_tuple_count(0), current_unique_count(0), current_dict_size(0) {
		current_set = make_unique<std::unordered_set<string>>();
	}

	size_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	size_t current_dict_size;
	unique_ptr<std::unordered_set<string>> current_set;

	bool LookupString(string str) override {
		return current_set->count(str) == 0;
	}

	void AddNewString(string_t str) override  {
		current_unique_count++;
		current_dict_size += str.GetString().size();
		current_set->insert(str.GetString());
	}

	void AddLastLookup() override  {
		current_tuple_count++;
	}

	void AddNull() override {
		current_tuple_count++;
	}

	bool HasEnoughSpace(bool new_string, size_t string_size) override {
		auto new_width = BitpackingPrimitives::MinimumBitWidth(current_unique_count + 1 + new_string);
		if (new_string){
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count+1, current_unique_count+1, current_dict_size + string_size, new_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count+1, current_unique_count, current_dict_size, new_width);
		}
	}

	void Flush() override {
		segment_count++;
		current_tuple_count = 0;
		current_unique_count = 0;
		current_dict_size = 0;
		current_set = make_unique<std::unordered_set<string>>();
	}
	void Verify() override {};
};

unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<DictionaryCompressionAnalyzeState>();
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;
	try {
		state.UpdateState(input, count);
	} catch(NotImplementedException &e) {
		return false;
	}
	return true;
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;

	auto width = BitpackingPrimitives::MinimumBitWidth(state.current_unique_count + 1);
	auto req_space = RequiredSpace(state.current_tuple_count, state.current_unique_count, state.current_dict_size, width);

	return state.segment_count * Storage::BLOCK_SIZE + req_space;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                           unique_ptr<AnalyzeState> state) {
	return make_unique<DictionaryCompressionCompressState>(checkpointer);
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (DictionaryCompressionCompressState &)state_p;
	state.UpdateState(scan_vector, count);
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = (DictionaryCompressionCompressState &)state_p;
	state.Flush();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public StringScanState {
	unique_ptr<BufferHandle> handle;
	buffer_ptr<Vector> dictionary;
	bitpacking_width_t current_width;
	unique_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
};

unique_ptr<SegmentScanState> DictionaryCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_unique<CompressedStringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);

	// TODO: we should consider if emitting dictionary vectors is always a good idea, maybe not?
	// TODO: we could use index_buffer_count to estimate the compression ratio and switch to emitting dict vectors when
	// TODO: compression ratio is high
	auto baseptr = state->handle->node->buffer + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, *(state->handle));
	auto index_buffer_offset = Load<uint32_t>(baseptr + 2 * sizeof(uint32_t));
	auto index_buffer_count = Load<uint32_t>(baseptr + 3 * sizeof(uint32_t));
	state->current_width = (bitpacking_width_t)(Load<uint32_t>(baseptr + 4 * sizeof(uint32_t)));
	auto index_buffer_ptr = (int32_t *)(baseptr + index_buffer_offset);

	state->dictionary = make_buffer<Vector>(LogicalType::VARCHAR, index_buffer_count);
	auto dict_child_data = FlatVector::GetData<string_t>(*(state->dictionary));

	// TODO the first value in the index buffer is always 0, do we care?
	for (uint32_t i = 0; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = GetStringLength(index_buffer_ptr, i);
		dict_child_data[i] =
		    FetchStringFromDict(segment, dict, *(state->dictionary), baseptr, index_buffer_ptr[i], str_len);
	}

	return move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = (CompressedStringScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, *scan_state.handle);

	auto index_buffer_offset = Load<uint32_t>(baseptr + 2 * sizeof(uint32_t));
	auto index_buffer_ptr = (int32_t *)(baseptr + index_buffer_offset);

	auto base_data = (data_ptr_t)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	// TODO: maybe we can do non-bitpacking-algorithm-group-aligned dict vector emitting? It might not be very common though
	if (scan_count != STANDARD_VECTOR_SIZE || start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
		// Emit regular vector

		// Handling non-bitpacking-group-aligned start values;
		idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

		// Create a decompression buffer of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_unique<SelectionVector>(decompress_count);
		}

		data_ptr_t src = (data_ptr_t)&base_data[((start - start_offset) * scan_state.current_width) / 8];
		sel_t *sel_vec_ptr = scan_state.sel_vec->data();

		BitpackingPrimitives::UnPackBuffer<sel_t>((data_ptr_t)sel_vec_ptr, src, decompress_count,
		                                          scan_state.current_width);

		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = scan_state.sel_vec->get_index(i + start_offset);
			auto dict_offset = index_buffer_ptr[string_number];
			uint16_t str_len = GetStringLength(index_buffer_ptr, string_number);
			result_data[result_offset + i] = FetchStringFromDict(segment, dict, result, baseptr, dict_offset, str_len);
		}

	} else {
		D_ASSERT(start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
		D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
		D_ASSERT(result_offset == 0);

		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count);

		// Scanning 1024 values, emitting a dict vector
		auto sel_data = make_buffer<SelectionData>(decompress_count);
		data_ptr_t dst = (data_ptr_t)(sel_data->owned_data.get());
		data_ptr_t src = (data_ptr_t)&base_data[(start * scan_state.current_width) / 8];

		BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, scan_count, scan_state.current_width);

		SelectionVector sel_vector(sel_data);
		result.Slice(*(scan_state.dictionary), sel_vector, scan_count);
	}
}

void DictionaryCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                              Vector &result) {
	StringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                  Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto primary_id = segment.block->BlockId();

	BufferHandle *handle_ptr;
	auto entry = state.handles.find(primary_id);
	if (entry == state.handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		auto handle = buffer_manager.Pin(segment.block);
		handle_ptr = handle.get();
		state.handles[primary_id] = move(handle);
	} else {
		// already pinned: use the pinned handle
		handle_ptr = entry->second.get();
	}

	auto baseptr = handle_ptr->node->buffer + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, *handle_ptr);
	auto index_buffer_offset = Load<uint32_t>(baseptr + 2 * sizeof(uint32_t));
	auto width = (bitpacking_width_t)(Load<uint32_t>(baseptr + 4 * sizeof(uint32_t)));
	auto index_buffer_ptr = (int32_t *)(baseptr + index_buffer_offset);
	auto base_data = (data_ptr_t)(baseptr + DICTIONARY_HEADER_SIZE);
	auto start = segment.GetRelativeIndex(row_id);
	auto result_data = FlatVector::GetData<string_t>(result);

	auto group_size = BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = start % group_size;

	// Make decompression buffer todo: should not be sel vec i guess
	auto sel_vec = make_unique<SelectionVector>(group_size);
	data_ptr_t src = (data_ptr_t)&base_data[((start - start_offset) * width) / 8];
	sel_t *sel_vec_ptr = sel_vec->data();

	BitpackingPrimitives::UnPackBuffer<sel_t>((data_ptr_t)sel_vec_ptr, src, group_size, width);

	auto string_number = sel_vec->get_index(start_offset);
	auto dict_offset = index_buffer_ptr[string_number];
	uint16_t str_len = GetStringLength(index_buffer_ptr, string_number);

	result_data[result_idx] = FetchStringFromDict(segment, dict, result, baseptr, dict_offset, str_len);
}

//===--------------------------------------------------------------------===//
// Helper Functions TODO move out of DictionaryCompressionStorage
//===--------------------------------------------------------------------===//
bool DictionaryCompressionStorage::HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	return RequiredSpace(current_count, index_count, dict_size, packing_width) <= Storage::BLOCK_SIZE;
}

// TODO what if block has offset?
idx_t DictionaryCompressionStorage::RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	idx_t base_space = DICTIONARY_HEADER_SIZE + dict_size;
	idx_t string_number_space = BitpackingPrimitives::GetRequiredSize<sel_t>(current_count, packing_width);
	idx_t index_space = index_count * sizeof(uint32_t);

	idx_t used_space = base_space + index_space + string_number_space;

	return used_space;
}

string_t DictionaryCompressionStorage::FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict,
                                                           Vector &result, data_ptr_t baseptr, int32_t dict_offset,
                                                           uint16_t string_len) {
	D_ASSERT(dict_offset >= 0 && dict_offset <= Storage::BLOCK_SIZE);

	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}
	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = (char *)(dict_pos);
	return string_t(str_ptr, string_len);
}

uint16_t DictionaryCompressionStorage::GetStringLength(int32_t *index_buffer_ptr, int32_t string_number) {
	if (string_number == 0) {
		return 0;
	} else {
		return index_buffer_ptr[string_number] - index_buffer_ptr[string_number - 1];
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
	    DictionaryCompressionStorage::StringScan, DictionaryCompressionStorage::StringScanPartial,
	    DictionaryCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DictionaryCompressionFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}
} // namespace duckdb