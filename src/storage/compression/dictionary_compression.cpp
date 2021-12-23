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

struct DictionaryCompressionState : UncompressedCompressState {
	explicit DictionaryCompressionState(ColumnDataCheckpointer &checkpointer)
	    : UncompressedCompressState(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_DICTIONARY, PhysicalType::VARCHAR);

		// TODO: deduplicate this?
		current_segment->function = function;
		ResetBuffers();
	}

	void CreateEmptySegment(idx_t row_start) override {
		// TODO: the overflow writer is not used, but created through this method
		UncompressedCompressState::CreateEmptySegment(row_start);
		current_segment->function = function;
		ResetBuffers();
	}

	void ResetBuffers() {
		current_string_map = make_unique<std::unordered_map<string, int32_t>>(); // TODO: initialization size?
		index_buffer = make_unique<std::vector<int32_t>>();                      // TODO: initialization size?
		index_buffer->push_back(0);                                              // For NULL strings
		selection_buffer = make_unique<std::vector<int32_t>>();
	}
	// TODO initialize with certain size?
	unique_ptr<std::unordered_map<string, int32_t>> current_string_map;
	unique_ptr<std::vector<int32_t>> index_buffer;
	unique_ptr<std::vector<int32_t>> selection_buffer;
	CompressionFunction *function;

	// Minimal bitpacking width for the indices in the index buffer. Necessary for current size on disk
	bitpacking_width_t min_width = sizeof(sel_t) * 8;
};

struct DictionaryCompressionStorage : UncompressedStringStorage {
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

	static idx_t StringAppendCompressed(ColumnSegment &segment, SegmentStatistics &stats, VectorData &data,
	                                    idx_t offset, idx_t count, DictionaryCompressionState &state);

	static idx_t FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats, DictionaryCompressionState &state);

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

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryCompressionAnalyzeState : public AnalyzeState {
	DictionaryCompressionAnalyzeState() : count(0), total_string_size(0), overflow_strings(0), current_segment_fill(0) {
		current_string_map = make_unique<std::unordered_map<string, int32_t>>();
	}

	idx_t count;
	idx_t total_string_size;
	idx_t overflow_strings;

	unique_ptr<std::unordered_map<string, int32_t>> current_string_map;
	size_t current_segment_fill;
};

unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<DictionaryCompressionAnalyzeState>();
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;
	VectorData vdata;
	input.Orrify(count, vdata);

	// TODO test analysis
	state.count += count;
	auto data = (string_t *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		state.current_segment_fill += sizeof(int32_t);
		if (vdata.validity.RowIsValid(idx)) {
			auto string_size = data[idx].GetSize();

			// TODO: should we reconsider this? or even support big strings?
			if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
				return false;
			}

			if (state.current_string_map->count(data[idx].GetString()) == 0) {
				state.total_string_size += string_size;
				state.current_segment_fill += string_size;
				state.current_string_map->insert({data[idx].GetString(), string_size});
			}

			// TODO can we do better than this in size estimation?
			if (state.current_segment_fill >= Storage::BLOCK_SIZE) {

				// Clearing the string map is crazy slow for poorly compressible data.
				//				state.current_string_map->clear();
				state.current_string_map.reset();
				state.current_string_map = make_unique<std::unordered_map<string, int32_t>>();
			}
		}
	}
	return true;
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;
	// TODO improve analysis
	return state.count * sizeof(int32_t) + state.total_string_size + state.overflow_strings * BIG_STRING_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                           unique_ptr<AnalyzeState> state) {
	return make_unique<DictionaryCompressionState>(checkpointer);
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (DictionaryCompressionState &)state_p;

	// TODO: check for GetBlockOFfset in other parts of code and then remove them all or add them all then remove
	// asserts?
	D_ASSERT(state.current_segment->GetBlockOffset() == 0);

	VectorData vdata;
	scan_vector.Orrify(count, vdata);

	ColumnAppendState append_state;
	idx_t offset = 0;
	while (count > 0) {
		idx_t appended =
		    StringAppendCompressed(*state.current_segment, state.current_segment->stats, vdata, offset, count, state);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk

		state.FlushSegment(
		    DictionaryCompressionStorage::FinalizeAppend(*state.current_segment, state.current_segment->stats, state));

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = (DictionaryCompressionState &)state_p;
	state.Finalize(
	    DictionaryCompressionStorage::FinalizeAppend(*state.current_segment, state.current_segment->stats, state));
}

//===--------------------------------------------------------------------===//
// Compressed Append
//===--------------------------------------------------------------------===//

// NOTE: this does not support overflow blocks
// TODO should I rename this? its not an actual append method
idx_t DictionaryCompressionStorage::StringAppendCompressed(ColumnSegment &segment, SegmentStatistics &stats,
                                                           VectorData &data, idx_t offset, idx_t count,
                                                           DictionaryCompressionState &state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto source_data = (string_t *)data.data;
	auto dictionary = GetDictionary(segment, *handle);

	// TODO we only support adding to fresh blocks?
	D_ASSERT(segment.GetBlockOffset() == 0);
	D_ASSERT(segment.count == state.selection_buffer->size());

	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(HasEnoughSpace(segment.count.load(), state.index_buffer->size(), dictionary.size, state.min_width));
		auto source_idx = data.sel->get_index(offset + i);
		auto target_idx = segment.count.load();

		// Check if adding a single sel_t would overfill the segment
		if (!HasEnoughSpace(target_idx + 1, state.index_buffer->size(), dictionary.size, state.min_width)) {
			return i;
		}

		if (!data.validity.RowIsValid(source_idx)) {
			state.selection_buffer->push_back(0);
		} else {
			auto end = handle->node->buffer + dictionary.end;

			dictionary.Verify();

			auto search = state.current_string_map->find(source_data[source_idx].GetString());

			if (search != state.current_string_map->end()) {
				state.selection_buffer->push_back(search->second);
			} else {
				// Unknown string, continue

				sel_t latest_inserted_sel = state.index_buffer->size();
				bitpacking_width_t new_width = BitpackingPrimitives::MinimumBitWidth(&latest_inserted_sel, 1);

				// non-null value, check if we can fit it within the block
				idx_t string_length = source_data[source_idx].GetSize();
				idx_t dictionary_length = string_length;

				idx_t required_space = dictionary_length + sizeof(int32_t); // for index_buffer value

				if (required_space >= StringUncompressed::STRING_BLOCK_LIMIT) {
					throw InternalException("Overflow strings not supported");
				}

				// Test if the new string fits.
				if (!HasEnoughSpace(target_idx + 1, state.index_buffer->size(), dictionary.size + required_space,
				                    new_width)) {
					return i;
				}

				UpdateStringStats(stats, source_data[source_idx]);

				// string fits in block, append to dictionary and increment dictionary position
				D_ASSERT(string_length < NumericLimits<uint16_t>::Maximum());
				dictionary.size += dictionary_length;
				auto dict_pos = end - dictionary.size;
				// now write the actual string data into the dictionary
				memcpy(dict_pos, source_data[source_idx].GetDataUnsafe(), string_length);
				dictionary.Verify();

				// write selection value, string index, and updated bitpacking width
				state.index_buffer->push_back(dictionary.size);
				state.selection_buffer->push_back(state.index_buffer->size() - 1);
				state.current_string_map->insert({source_data[source_idx].GetString(), state.index_buffer->size() - 1});
				state.min_width = new_width;

				SetDictionary(segment, *handle, dictionary);

				D_ASSERT(HasEnoughSpace(target_idx + 1, state.index_buffer->size(), dictionary.size, state.min_width));
			}
		}
		segment.count++;
		dictionary.Verify();

		D_ASSERT(state.selection_buffer->size() == segment.count);
		D_ASSERT(state.index_buffer->size() == state.current_string_map->size() + 1);
		D_ASSERT(HasEnoughSpace(segment.count, state.index_buffer->size(), dictionary.size, state.min_width));
	}
	return count;
}

// TODO this is not an actual finalizeAPPEnd, rename into something more clear?
idx_t DictionaryCompressionStorage::FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats,
                                                   DictionaryCompressionState &state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dict = GetDictionary(segment, *handle);
	D_ASSERT(dict.end == Storage::BLOCK_SIZE);

	// compute the total size required to store this segment
	auto index_buffer_size = state.index_buffer->size() * sizeof(int32_t); // TODO: is this really the required size?
	auto offset_size =
	    DICTIONARY_HEADER_SIZE + BitpackingPrimitives::GetRequiredSize<sel_t>(segment.count, state.min_width);
	auto total_size = offset_size + dict.size + index_buffer_size;

	// Write the index buffer next to the index buffer offsets and store its offset in the header
	memcpy(handle->node->buffer + offset_size, state.index_buffer->data(), index_buffer_size);
	Store<uint32_t>(offset_size, handle->node->buffer + 2 * sizeof(uint32_t)); // TODO segment offset?
	Store<uint32_t>(state.index_buffer->size(), handle->node->buffer + 3 * sizeof(uint32_t)); // TODO segment offset?
  	Store<uint32_t>((uint32_t)state.min_width, handle->node->buffer + 4 * sizeof(uint32_t));

	D_ASSERT(segment.GetBlockOffset() == 0); // TODO segment offset?
	D_ASSERT(HasEnoughSpace(segment.count, state.index_buffer->size(), dict.size, state.min_width));
	D_ASSERT((uint64_t)*max_element(std::begin(*state.selection_buffer), std::end(*state.selection_buffer)) <
	         state.index_buffer->size());

	auto baseptr = handle->node->buffer + segment.GetBlockOffset();
	data_ptr_t dst = baseptr + DICTIONARY_HEADER_SIZE;
	sel_t *src = (sel_t *)(state.selection_buffer->data());

	// Note: PackBuffer may write beyond dst+count*width, however BitpackingPrimitives::GetRequiredSize, has made sure
	// we have enough space in dst
	BitpackingPrimitives::PackBuffer<sel_t, false>(dst, src, segment.count, state.min_width);

	// TODO: deduplicate with uncompressed_string?
	if (total_size >= Storage::BLOCK_SIZE / 5 * 4) {
		// the block is full enough, don't bother moving around the dictionary
		return Storage::BLOCK_SIZE;
	}
	// the block has space left: figure out how much space we can save
	auto move_amount = Storage::BLOCK_SIZE - total_size;
	// move the dictionary so it lines up exactly with the offsets
	memmove(handle->node->buffer + offset_size + index_buffer_size, handle->node->buffer + dict.end - dict.size,
	        dict.size);
	dict.end -= move_amount;
	D_ASSERT(dict.end == total_size);
	// write the new dictionary (with the updated "end")
	SetDictionary(segment, *handle, dict);
	return total_size;
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

		// We will scan in blocks of 32 so we may scan some extra values.
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
// Helper Functions
//===--------------------------------------------------------------------===//

bool DictionaryCompressionStorage::HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	return RequiredSpace(current_count, index_count, dict_size, packing_width) <= Storage::BLOCK_SIZE;
}

// TODO what if block has offset?
idx_t DictionaryCompressionStorage::RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	//	D_ASSERT(dictionary.end == Storage::BLOCK_SIZE); TODO do somewhere else?

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