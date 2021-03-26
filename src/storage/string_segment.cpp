#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

StringSegment::StringSegment(DatabaseInstance &db, idx_t row_start, block_id_t block_id)
    : UncompressedSegment(db, PhysicalType::VARCHAR, row_start) {
	this->max_vector_count = 0;
	// the vector_size is given in the size of the dictionary offsets
	this->vector_size = STANDARD_VECTOR_SIZE * sizeof(int32_t);

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id == INVALID_BLOCK) {
		// start off with an empty string segment: allocate space for it
		this->block = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
		auto handle = buffer_manager.Pin(block);
		SetDictionaryOffset(*handle, sizeof(idx_t));

		ExpandStringSegment(handle->node->buffer);
	} else {
		this->block = buffer_manager.RegisterBlock(block_id);
	}
}

void StringSegment::SetDictionaryOffset(BufferHandle &handle, idx_t offset) {
	Store<idx_t>(offset, handle.node->buffer + Storage::BLOCK_SIZE - sizeof(idx_t));
}

idx_t StringSegment::GetDictionaryOffset(BufferHandle &handle) {
	return Load<idx_t>(handle.node->buffer + Storage::BLOCK_SIZE - sizeof(idx_t));
}

StringSegment::~StringSegment() {
	while (head) {
		// prevent deep recursion here
		head = move(head->next);
	}
}

void StringSegment::ExpandStringSegment(data_ptr_t baseptr) {
	// clear the nullmask for this vector
	max_vector_count++;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void StringSegment::InitializeScan(ColumnScanState &state) {
	// pin the primary buffer
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	state.primary_handle = buffer_manager.Pin(block);
}

//===--------------------------------------------------------------------===//
// Filter base data
//===--------------------------------------------------------------------===//
void StringSegment::ReadString(string_t *result_data, Vector &result, data_ptr_t baseptr, int32_t *dict_offset,
                               idx_t src_idx, idx_t res_idx, idx_t &update_idx, size_t vector_index) {
	result_data[res_idx] = FetchStringFromDict(result, baseptr, dict_offset[src_idx]);
}

//===--------------------------------------------------------------------===//
// Fetch base data
//===--------------------------------------------------------------------===//
void StringSegment::FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) {
	// clear any previously locked buffers and get the primary buffer handle
	auto handle = state.primary_handle.get();

	// fetch the data from the base segment
	FetchBaseData(state, handle->node->buffer, vector_index, result, GetVectorCount(vector_index));
}

void StringSegment::FetchBaseData(ColumnScanState &state, data_ptr_t baseptr, idx_t vector_index, Vector &result,
                                  idx_t count) {
	auto base = baseptr + vector_index * vector_size;

	auto base_data = (int32_t *)base;
	auto result_data = FlatVector::GetData<string_t>(result);

	// no updates: fetch only from the string dictionary
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = FetchStringFromDict(result, baseptr, base_data[i]);
	}
}

//===--------------------------------------------------------------------===//
// Fetch strings
//===--------------------------------------------------------------------===//
void StringSegment::FetchStringLocations(data_ptr_t baseptr, row_t *ids, idx_t vector_index, idx_t vector_offset,
                                         idx_t count, string_location_t result[]) {
	auto base = baseptr + vector_index * vector_size;
	auto base_data = (int32_t *)base;

	// no updates: fetch strings from base vector
	for (idx_t i = 0; i < count; i++) {
		auto id = ids[i] - vector_offset;
		result[i] = FetchStringLocation(baseptr, base_data[id]);
	}
}

string_location_t StringSegment::FetchStringLocation(data_ptr_t baseptr, int32_t dict_offset) {
	D_ASSERT(dict_offset >= 0 && dict_offset <= Storage::BLOCK_ALLOC_SIZE);
	if (dict_offset == 0) {
		return string_location_t(INVALID_BLOCK, 0);
	}
	// look up result in dictionary
	auto dict_end = baseptr + Storage::BLOCK_SIZE;
	auto dict_pos = dict_end - dict_offset;
	auto string_length = Load<uint16_t>(dict_pos);
	string_location_t result;
	if (string_length == BIG_STRING_MARKER) {
		ReadStringMarker(dict_pos, result.block_id, result.offset);
	} else {
		result.block_id = INVALID_BLOCK;
		result.offset = dict_offset;
	}
	return result;
}

string_t StringSegment::FetchStringFromDict(Vector &result, data_ptr_t baseptr, int32_t dict_offset) {
	// fetch base data
	D_ASSERT(dict_offset <= Storage::BLOCK_SIZE);
	string_location_t location = FetchStringLocation(baseptr, dict_offset);
	return FetchString(result, baseptr, location);
}

string_t StringSegment::FetchString(Vector &result, data_ptr_t baseptr, string_location_t location) {
	if (location.block_id != INVALID_BLOCK) {
		// big string marker: read from separate block
		return ReadString(result, location.block_id, location.offset);
	} else {
		if (location.offset == 0) {
			return string_t(nullptr, 0);
		}
		// normal string: read string from this block
		auto dict_end = baseptr + Storage::BLOCK_SIZE;
		auto dict_pos = dict_end - location.offset;
		auto string_length = Load<uint16_t>(dict_pos);

		auto str_ptr = (char *)(dict_pos + sizeof(uint16_t));
		return string_t(str_ptr, string_length);
	}
}

void StringSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	idx_t vector_index = row_id / STANDARD_VECTOR_SIZE;
	idx_t id_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;
	D_ASSERT(vector_index < max_vector_count);

	data_ptr_t baseptr;

	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto primary_id = block->BlockId();

	auto entry = state.handles.find(primary_id);
	if (entry == state.handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		auto handle = buffer_manager.Pin(block);
		baseptr = handle->node->buffer;
		state.handles[primary_id] = move(handle);
	} else {
		// already pinned: use the pinned handle
		baseptr = entry->second->node->buffer;
	}

	auto base = baseptr + vector_index * vector_size;
	auto base_data = (int32_t *)base;
	auto result_data = FlatVector::GetData<string_t>(result);

	result_data[result_idx] = FetchStringFromDict(result, baseptr, base_data[id_in_vector]);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t StringSegment::Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);
	idx_t initial_count = tuple_count;
	while (count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		idx_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			// we are at the maximum vector, check if there is space to increase the maximum vector count
			// as a heuristic, we only allow another vector to be added if we have at least 32 bytes per string
			// remaining (32KB out of a 256KB block, or around 12% empty)
			if (RemainingSpace(*handle) >= STANDARD_VECTOR_SIZE * 32) {
				// we have enough remaining space to add another vector
				ExpandStringSegment(handle->node->buffer);
			} else {
				break;
			}
		}
		idx_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		idx_t append_count = MinValue(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		AppendData(*handle, stats, handle->node->buffer + vector_size * vector_index,
		           handle->node->buffer + Storage::BLOCK_SIZE, current_tuple_count, data, offset, append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

idx_t StringSegment::RemainingSpace(BufferHandle &handle) {
	idx_t used_space = GetDictionaryOffset(handle) + max_vector_count * vector_size;
	D_ASSERT(Storage::BLOCK_SIZE >= used_space);
	return Storage::BLOCK_SIZE - used_space;
}

static inline void UpdateStringStats(SegmentStatistics &stats, const string_t &new_value) {
	auto &sstats = (StringStatistics &)*stats.statistics;
	sstats.Update(new_value);
}

void StringSegment::AppendData(BufferHandle &handle, SegmentStatistics &stats, data_ptr_t target, data_ptr_t end,
                               idx_t target_offset, VectorData &adata, idx_t offset, idx_t count) {
	auto sdata = (string_t *)adata.data;
	auto result_data = (int32_t *)target;

	idx_t remaining_strings = STANDARD_VECTOR_SIZE - (this->tuple_count % STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = adata.sel->get_index(offset + i);
		auto target_idx = target_offset + i;
		if (!adata.validity.RowIsValid(source_idx)) {
			// null value is stored as -1
			result_data[target_idx] = 0;
		} else {
			auto dictionary_offset = GetDictionaryOffset(handle);
			D_ASSERT(dictionary_offset < Storage::BLOCK_SIZE);
			// non-null value, check if we can fit it within the block
			idx_t string_length = sdata[source_idx].GetSize();
			idx_t total_length = string_length + sizeof(uint16_t);

			UpdateStringStats(stats, sdata[source_idx]);

			// determine whether or not the string needs to be stored in an overflow block
			// we never place small strings in the overflow blocks: the pointer would take more space than the
			// string itself we always place big strings (>= STRING_BLOCK_LIMIT) in the overflow blocks we also have
			// to always leave enough room for BIG_STRING_MARKER_SIZE for each of the remaining strings
			if (total_length > BIG_STRING_MARKER_BASE_SIZE &&
			    (total_length >= STRING_BLOCK_LIMIT ||
			     total_length + (remaining_strings * BIG_STRING_MARKER_SIZE) > RemainingSpace(handle))) {
				D_ASSERT(RemainingSpace(handle) >= BIG_STRING_MARKER_SIZE);
				// string is too big for block: write to overflow blocks
				block_id_t block;
				int32_t offset;
				// write the string into the current string block
				WriteString(sdata[source_idx], block, offset);
				dictionary_offset += BIG_STRING_MARKER_SIZE;
				auto dict_pos = end - dictionary_offset;

				// write a big string marker into the dictionary
				WriteStringMarker(dict_pos, block, offset);
			} else {
				// string fits in block, append to dictionary and increment dictionary position
				D_ASSERT(string_length < NumericLimits<uint16_t>::Maximum());
				dictionary_offset += total_length;
				auto dict_pos = end - dictionary_offset; // first write the length as u16
				Store<uint16_t>(string_length, dict_pos);
				// now write the actual string data into the dictionary
				memcpy(dict_pos + sizeof(uint16_t), sdata[source_idx].GetDataUnsafe(), string_length);
			}
			D_ASSERT(RemainingSpace(handle) <= Storage::BLOCK_SIZE);
			// place the dictionary offset into the set of vectors
			D_ASSERT(dictionary_offset <= Storage::BLOCK_SIZE);
			result_data[target_idx] = dictionary_offset;
			SetDictionaryOffset(handle, dictionary_offset);
		}
		remaining_strings--;
	}
}

void StringSegment::WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) {
	if (overflow_writer) {
		// overflow writer is set: write string there
		overflow_writer->WriteString(string, result_block, result_offset);
	} else {
		// default overflow behavior: use in-memory buffer to store the overflow string
		WriteStringMemory(string, result_block, result_offset);
	}
}

void StringSegment::WriteStringMemory(string_t string, block_id_t &result_block, int32_t &result_offset) {
	uint32_t total_length = string.GetSize() + sizeof(uint32_t);
	shared_ptr<BlockHandle> block;
	unique_ptr<BufferHandle> handle;

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	// check if the string fits in the current block
	if (!head || head->offset + total_length >= head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		idx_t alloc_size = MaxValue<idx_t>(total_length, Storage::BLOCK_ALLOC_SIZE);
		auto new_block = make_unique<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		block = buffer_manager.RegisterMemory(alloc_size, false);
		handle = buffer_manager.Pin(block);
		overflow_blocks[block->BlockId()] = new_block.get();
		new_block->block = move(block);
		new_block->next = move(head);
		head = move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = buffer_manager.Pin(head->block);
	}

	result_block = head->block->BlockId();
	result_offset = head->offset;

	// copy the string and the length there
	auto ptr = handle->node->buffer + head->offset;
	Store<uint32_t>(string.GetSize(), ptr);
	ptr += sizeof(uint32_t);
	memcpy(ptr, string.GetDataUnsafe(), string.GetSize());
	head->offset += total_length;
}

string_t StringSegment::ReadString(Vector &result, block_id_t block, int32_t offset) {
	D_ASSERT(offset < Storage::BLOCK_SIZE);
	if (block == INVALID_BLOCK) {
		return string_t(nullptr, 0);
	}
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block < MAXIMUM_BLOCK) {
		// read the overflow string from disk
		// pin the initial handle and read the length
		auto block_handle = buffer_manager.RegisterBlock(block);
		auto handle = buffer_manager.Pin(block_handle);

		uint32_t length = Load<uint32_t>(handle->node->buffer + offset);
		uint32_t remaining = length;
		offset += sizeof(uint32_t);

		// allocate a buffer to store the string
		auto alloc_size = MaxValue<idx_t>(Storage::BLOCK_ALLOC_SIZE, length + sizeof(uint32_t));
		auto target_handle = buffer_manager.Allocate(alloc_size);
		auto target_ptr = target_handle->node->buffer;
		// write the length in this block as well
		Store<uint32_t>(length, target_ptr);
		target_ptr += sizeof(uint32_t);
		// now append the string to the single buffer
		while (remaining > 0) {
			idx_t to_write = MinValue<idx_t>(remaining, Storage::BLOCK_SIZE - sizeof(block_id_t) - offset);
			memcpy(target_ptr, handle->node->buffer + offset, to_write);

			remaining -= to_write;
			offset += to_write;
			target_ptr += to_write;
			if (remaining > 0) {
				// read the next block
				block_id_t next_block = Load<block_id_t>(handle->node->buffer + offset);
				block_handle = buffer_manager.RegisterBlock(next_block);
				handle = buffer_manager.Pin(block_handle);
				offset = 0;
			}
		}

		auto final_buffer = target_handle->node->buffer;
		StringVector::AddHandle(result, move(target_handle));
		return ReadString(final_buffer, 0);
	} else {
		// read the overflow string from memory
		// first pin the handle, if it is not pinned yet
		auto entry = overflow_blocks.find(block);
		D_ASSERT(entry != overflow_blocks.end());
		auto handle = buffer_manager.Pin(entry->second->block);
		auto final_buffer = handle->node->buffer;
		StringVector::AddHandle(result, move(handle));
		return ReadString(final_buffer, offset);
	}
}

string_t StringSegment::ReadString(data_ptr_t target, int32_t offset) {
	auto ptr = target + offset;
	auto str_length = Load<uint32_t>(ptr);
	auto str_ptr = (char *)(ptr + sizeof(uint32_t));
	return string_t(str_ptr, str_length);
}

void StringSegment::WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	uint16_t length = BIG_STRING_MARKER;
	memcpy(target, &length, sizeof(uint16_t));
	target += sizeof(uint16_t);
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

void StringSegment::ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset) {
	target += sizeof(uint16_t);
	memcpy(&block_id, target, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(&offset, target, sizeof(int32_t));
}

void StringSegment::ToTemporary() {
	ToTemporaryInternal();
	this->max_vector_count = (this->tuple_count + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
}

} // namespace duckdb
