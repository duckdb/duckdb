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
		current_segment->function = function;
		current_string_map = make_unique<std::unordered_map<string, int32_t>>();
		index_buffer = make_unique<std::vector<int32_t>>();
		index_buffer->push_back(0); // For NULL strings TODO check nullable on column?
	}

	void CreateEmptySegment(idx_t row_start) override {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		if (type.InternalType() == PhysicalType::VARCHAR) {
			auto &state = (UncompressedStringSegmentState &)*compressed_segment->GetSegmentState();
			state.overflow_writer = make_unique<WriteOverflowStringsToDisk>(db);
		}
		current_segment = move(compressed_segment);

		// Reset the string map
		// Clearing the string map is crazy slow for poorly compressible data, likely due to random access.
		//		current_string_map->clear();
		current_string_map.reset();
		current_string_map = make_unique<std::unordered_map<string, int32_t>>();
		index_buffer.reset();
		index_buffer = make_unique<std::vector<int32_t>>();
		index_buffer->push_back(0); // For NULL strings
	}

	// TODO initialize with certain size?
	unique_ptr<std::unordered_map<string, int32_t>> current_string_map;
	unique_ptr<std::vector<int32_t>> index_buffer;
	CompressionFunction *function;
};

struct DictionaryCompressionStorage : UncompressedStringStorage {

	// We override this to make space to store the offset to the index buffer
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static idx_t StringAppendCompressed(ColumnSegment &segment, SegmentStatistics &stats, VectorData &data,
	                                    idx_t offset, idx_t count, std::unordered_map<string, int32_t> *seen_strings,
	                                    std::vector<int32_t> *index_buffer);

	static idx_t FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats, DictionaryCompressionState &state);

	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static idx_t RemainingSpace(ColumnSegment &segment, BufferHandle &handle, std::vector<int32_t> *index_buffer);
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

			if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
				state.overflow_strings++;
				state.current_segment_fill += BIG_STRING_MARKER_SIZE;
			}

			if (state.current_string_map->count(data[idx].GetString()) == 0) {
				state.total_string_size += string_size;
				state.current_segment_fill += string_size;
				state.current_string_map->insert({data[idx].GetString(), string_size});
			}

			// If we have filled a segment size worth of data, we clear the string map to simulate a new segment being
			// used
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
	VectorData vdata;
	scan_vector.Orrify(count, vdata);

	ColumnAppendState append_state;
	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = StringAppendCompressed(*state.current_segment, state.current_segment->stats, vdata, offset,
		                                        count, state.current_string_map.get(), state.index_buffer.get());
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk

		// TODO directly calling the finalize append may be hacky?
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
idx_t DictionaryCompressionStorage::StringAppendCompressed(ColumnSegment &segment, SegmentStatistics &stats,
                                                           VectorData &data, idx_t offset, idx_t count,
                                                           std::unordered_map<string, int32_t> *seen_strings,
                                                           std::vector<int32_t> *index_buffer) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	D_ASSERT(segment.GetBlockOffset() == 0);
	auto source_data = (string_t *)data.data;
	auto result_data = (int32_t *)(handle->node->buffer + DICTIONARY_HEADER_SIZE);

	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(RemainingSpace(segment, *handle, index_buffer) <= Storage::BLOCK_SIZE);
		auto source_idx = data.sel->get_index(offset + i);
		auto target_idx = segment.count.load();
		idx_t remaining_space = RemainingSpace(segment, *handle, index_buffer);
		if (remaining_space < sizeof(int32_t)) {
			// string index does not fit in the block at all
			return i;
		}

		remaining_space -= sizeof(int32_t);

		if (!data.validity.RowIsValid(source_idx)) {
			// null value is stored as -1
			result_data[target_idx] = 0;
		} else {
			auto dictionary = GetDictionary(segment, *handle);
			auto end = handle->node->buffer + dictionary.end;

			dictionary.Verify();

			// Lookup string TODO, GetString makes a copy I think, we can probably prevent that through some custom map
			auto search = seen_strings->find(source_data[source_idx].GetString());

			if (search != seen_strings->end()) {
				// We have seen this string
				result_data[target_idx] = search->second;
				D_ASSERT(RemainingSpace(segment, *handle, index_buffer) <= Storage::BLOCK_SIZE);
			} else {
				// Unknown string, continue
				// non-null value, check if we can fit it within the block
				idx_t string_length = source_data[source_idx].GetSize();
				idx_t dictionary_length = string_length + sizeof(uint16_t);

				idx_t required_space = dictionary_length + sizeof(int32_t); // for index_buffer value
				if (required_space >= StringUncompressed::STRING_BLOCK_LIMIT) {
					// string exceeds block limit, store in overflow block and only write a marker here
					throw InternalException("GTFO with your overflow strings");
				}
				if (required_space > remaining_space) {
					// no space remaining: return how many tuples we ended up writing
					return i;
				}

				// we have space: write the string
				UpdateStringStats(stats, source_data[source_idx]);

				// string fits in block, append to dictionary and increment dictionary position
				D_ASSERT(string_length < NumericLimits<uint16_t>::Maximum());
				dictionary.size += dictionary_length;

				// first write the length as u16
				auto dict_pos = end - dictionary.size;
				Store<uint16_t>(string_length, dict_pos);
				// now write the actual string data into the dictionary
				memcpy(dict_pos + sizeof(uint16_t), source_data[source_idx].GetDataUnsafe(), string_length);

				dictionary.Verify();

				index_buffer->push_back(dictionary.size);
				result_data[target_idx] = index_buffer->size() - 1;
				seen_strings->insert({source_data[source_idx].GetString(), index_buffer->size() - 1});
				SetDictionary(segment, *handle, dictionary);

				D_ASSERT(RemainingSpace(segment, *handle, index_buffer) <= Storage::BLOCK_SIZE);
			}
		}
		segment.count++;
	}
	return count;
}

idx_t DictionaryCompressionStorage::FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats,
                                                   DictionaryCompressionState &state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dict = GetDictionary(segment, *handle);
	D_ASSERT(dict.end == Storage::BLOCK_SIZE);

	// compute the total size required to store this segment
	auto index_buffer_size = state.index_buffer->size() * sizeof(int32_t);
	auto offset_size = DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
	auto total_size = offset_size + dict.size + index_buffer_size;

	// Write the index buffer next to the index buffer offsets and store its offset in the header
	memcpy(handle->node->buffer + offset_size, state.index_buffer->data(), index_buffer_size);
	Store<uint32_t>(offset_size, handle->node->buffer + 2 * sizeof(uint32_t)); // TODO segment offset?

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
// Scan base data
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = (StringScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, *scan_state.handle);

	auto index_buffer_offset = Load<uint32_t>(baseptr + 2 * sizeof(uint32_t));
	auto index_buffer_ptr = (int32_t *)(baseptr + index_buffer_offset);

	auto base_data = (int32_t *)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < scan_count; i++) {
		// Lookup dict offset in index buffer
		auto dict_offset = index_buffer_ptr[base_data[start + i]];

		result_data[result_offset + i] = FetchStringFromDict(segment, dict, result, baseptr, dict_offset);
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

	auto index_buffer_offset = Load<uint32_t>(baseptr + 2 * sizeof(uint32_t));
	auto index_buffer_ptr = (int32_t *)(baseptr + index_buffer_offset);

	auto dict = GetDictionary(segment, *handle_ptr);
	auto base_data = (int32_t *)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	auto dict_offset = index_buffer_ptr[base_data[row_id]];

	result_data[result_idx] = FetchStringFromDict(segment, dict, result, baseptr, dict_offset);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
idx_t DictionaryCompressionStorage::RemainingSpace(ColumnSegment &segment, BufferHandle &handle,
                                                   std::vector<int32_t> *index_buffer) {
	auto dictionary = GetDictionary(segment, handle);
	D_ASSERT(dictionary.end == Storage::BLOCK_SIZE);
	idx_t used_space = dictionary.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE +
	                   index_buffer->size() * sizeof(int32_t);
	D_ASSERT(Storage::BLOCK_SIZE >= used_space);
	return Storage::BLOCK_SIZE - used_space;
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	return CompressionFunction(
	    CompressionType::COMPRESSION_DICTIONARY, data_type, DictionaryCompressionStorage ::StringInitAnalyze,
	    DictionaryCompressionStorage::StringAnalyze, DictionaryCompressionStorage::StringFinalAnalyze,
	    DictionaryCompressionStorage::InitCompression, DictionaryCompressionStorage::Compress,
	    DictionaryCompressionStorage::FinalizeCompress, UncompressedStringStorage::StringInitScan,
	    DictionaryCompressionStorage::StringScan, DictionaryCompressionStorage::StringScanPartial,
	    DictionaryCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DictionaryCompressionFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}
} // namespace duckdb