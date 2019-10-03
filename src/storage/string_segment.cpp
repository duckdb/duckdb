#include "storage/string_segment.hpp"
#include "storage/buffer_manager.hpp"
// #include "common/types/vector.hpp"
// #include "storage/table/append_state.hpp"
// #include "transaction/version_info.hpp"
// #include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

StringSegment::StringSegment(BufferManager &manager) :
	UncompressedSegment(manager, TypeId::VARCHAR) {
	this->max_vector_count = 0;
	this->dictionary_offset = 0;
	// the vector_size is given in the size of the dictionary offsets
	this->vector_size = STANDARD_VECTOR_SIZE * sizeof(int32_t) + sizeof(nullmask_t);
	this->string_updates = nullptr;

	// we allocate one block to hold the majority of the
	auto handle = manager.Allocate(BLOCK_SIZE);
	this->block_id = handle->block_id;

	ExpandStringSegment(handle->buffer->data.get());
}

void StringSegment::ExpandStringSegment(data_ptr_t baseptr) {
	// clear the nullmask for this vector
	auto mask = (nullmask_t*) (baseptr + (max_vector_count * vector_size));
	mask->reset();

	max_vector_count++;
	if (versions) {
		auto new_versions = unique_ptr<UpdateInfo*[]>(new UpdateInfo*[max_vector_count]);
		memcpy(new_versions.get(), versions.get(), (max_vector_count - 1) * sizeof(UpdateInfo*));
		new_versions[max_vector_count - 1] = nullptr;
		versions = move(new_versions);
	}

	if (string_updates) {
		auto new_string_updates = unique_ptr<string_update_info_t[]>(new string_update_info_t[max_vector_count]);
		for(index_t i = 0; i < max_vector_count - 1; i++) {
			new_string_updates[i] = move(string_updates[i]);
		}
		new_string_updates[max_vector_count - 1] = 0;
		string_updates = move(new_string_updates);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void StringSegment::InitializeScan(TransientScanState &state) {
	// pin the primary buffer
	state.primary_handle = manager.PinBuffer(block_id);
}

void StringSegment::Scan(Transaction &transaction, TransientScanState &state, index_t vector_index, Vector &result) {
	auto read_lock = lock.GetSharedLock();

	// clear any previously locked buffers and get the primary buffer handle
	auto handle = (ManagedBufferHandle*) state.primary_handle.get();
	state.handles.clear();

	index_t count = GetVectorCount(vector_index);

	// fetch the data from the base segment
	FetchBaseData(state, handle->buffer->data.get(), vector_index, result, count);
	if (versions && versions[vector_index]) {
		// fetch data from updates
		auto result_data = (char**) result.data;
		auto current = versions[vector_index];
		while(current) {
			if (current->version_number > transaction.start_time && current->version_number != transaction.transaction_id) {
				// these tuples were either committed AFTER this transaction started or are not committed yet, use tuples stored in this version
				auto info_data = (char**) current->tuple_data;
				for(index_t i = 0; i < current->N; i++) {
					result_data[current->tuples[i]] = info_data[i];
					result.nullmask[current->tuples[i]] = current->nullmask[current->tuples[i]];
				}
			}
			current = current->next;
		}
	}
}

void StringSegment::FetchBaseData(row_t *ids, index_t vector_index, index_t vector_offset, index_t count, Vector &result) {
	TransientScanState state;
	InitializeScan(state);

	auto handle = (ManagedBufferHandle*) state.primary_handle.get();
	FetchBaseData(state, handle->buffer->data.get(), ids, vector_index, vector_offset, result, count);
}

void StringSegment::FetchBaseData(TransientScanState &state, data_ptr_t baseptr, row_t *ids, index_t vector_index, index_t vector_offset, Vector &result, index_t count) {
	auto base = baseptr + vector_index * vector_size;

	auto &base_nullmask = *((nullmask_t*) base);
	auto base_data = (int32_t *) (base + sizeof(nullmask_t));
	auto result_data = (char**) result.data;

	if (string_updates && string_updates[vector_index]) {
		// there are updates: merge them in
		auto &info = *string_updates[vector_index];
		index_t update_idx = 0;
		for(index_t i = 0; i < count; i++) {
			auto id = ids[i] - vector_offset;
			while(update_idx < info.count && info.ids[update_idx] < id) {
				update_idx++;
			}
			if (update_idx < info.count && info.ids[update_idx] == id) {
				// use update info
				result_data[i] = ReadString(state, info.block_ids[update_idx], info.offsets[update_idx]).data;
				result.nullmask[id] = info.nullmask[info.ids[update_idx]];
				update_idx++;
			} else {
				// use base table info
				result_data[i] = FetchStringFromDict(state, baseptr, base_data[id]).data;
				result.nullmask[i] = base_nullmask[i];
			}
		}
	} else {
		// no updates: fetch strings from base vector
		for(index_t i = 0; i < count; i++) {
			auto id = ids[i] - vector_offset;
			result_data[i] = FetchStringFromDict(state, baseptr, base_data[id]).data;
		}
		result.nullmask = base_nullmask;
	}
	result.count = count;
}

void StringSegment::FetchBaseData(TransientScanState &state, data_ptr_t baseptr, index_t vector_index, Vector &result, index_t count) {
	auto base = baseptr + vector_index * vector_size;

	auto &base_nullmask = *((nullmask_t*) base);
	auto base_data = (int32_t *) (base + sizeof(nullmask_t));
	auto result_data = (char**) result.data;

	if (string_updates && string_updates[vector_index]) {
		// there are updates: merge them in
		auto &info = *string_updates[vector_index];
		index_t update_idx = 0;
		for(index_t i = 0; i < count; i++) {
			if (update_idx < info.count && info.ids[update_idx] == i) {
				// use update info
				result_data[i] = ReadString(state, info.block_ids[update_idx], info.offsets[update_idx]).data;
				result.nullmask[i] = info.nullmask[info.ids[update_idx]];
				update_idx++;
			} else {
				// use base table info
				result_data[i] = FetchStringFromDict(state, baseptr, base_data[i]).data;
				result.nullmask[i] = base_nullmask[i];
			}
		}
	} else {
		// no updates: fetch only from the string dictionary
		for(index_t i = 0; i < count; i++) {
			result_data[i] = FetchStringFromDict(state, baseptr, base_data[i]).data;
		}
		result.nullmask = base_nullmask;
	}
	result.count = count;
}

string_t StringSegment::FetchStringFromDict(TransientScanState &state, data_ptr_t baseptr, int32_t dict_offset) {
	if (dict_offset == 0) {
		// no offset provided: return nullptr
		return string_t(nullptr, 0);
	}
	// look up result in dictionary
	auto dict_end = baseptr + BLOCK_SIZE;
	auto dict_pos = dict_end - dict_offset;
	auto string_length = *((uint16_t*) dict_pos);
	if (string_length == BIG_STRING_MARKER) {
		// big string marker, read the block id and offset
		block_id_t block;
		int32_t offset;
		ReadStringMarker(dict_pos, block, offset);
		return ReadString(state, block, offset);
	} else {
		string_t result;
		result.length = string_length;
		result.data = (char*) (dict_pos + sizeof(uint16_t));
		return result;
	}

}

void StringSegment::IndexScan(TransientScanState &state, index_t vector_index, Vector &result) {
	throw Exception("FIXME");
}

void StringSegment::Fetch(index_t vector_index, Vector &result) {
	throw Exception("FIXME");
}

void StringSegment::Fetch(Transaction &transaction, row_t row_id, Vector &result) {
	throw Exception("FIXME");
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
index_t StringSegment::Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count) {
	assert(data.type == TypeId::VARCHAR);
	auto handle = manager.PinBuffer(block_id);

	index_t initial_count = tuple_count;
	while(count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		index_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			// we are at the maximum vector, check if there is space to increase the maximum vector count
			// as a heuristic, we only allow another vector to be added if we have at least 32 bytes per string remaining (32KB out of a 256KB block, or around 12% empty)
			index_t remaining_space = BLOCK_SIZE - dictionary_offset - max_vector_count * vector_size;
			if (remaining_space >= STANDARD_VECTOR_SIZE * 32) {
				// we have enough remaining space to add another vector
				ExpandStringSegment(handle->buffer->data.get());
			} else {
				break;
			}
		}
		index_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		index_t append_count = std::min(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		AppendData(stats, handle->buffer->data.get() + vector_size * vector_index, handle->buffer->data.get() + BLOCK_SIZE, current_tuple_count, data, offset, append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

void StringSegment::AppendData(SegmentStatistics &stats, data_ptr_t target, data_ptr_t end, index_t target_offset, Vector &source, index_t offset, index_t count) {
	assert(offset + count <= source.count);
	auto ldata = (char**)source.data;
	auto &result_nullmask = *((nullmask_t*) target);
	auto result_data = (int32_t *) (target + sizeof(nullmask_t));

	VectorOperations::Exec(source.sel_vector, count + offset, [&](index_t i, index_t k) {
		if (source.nullmask[i]) {
			// null value is stored as -1
			result_data[k - offset + target_offset] = 0;
			result_nullmask[k - offset + target_offset] = true;
			stats.has_null = true;
		} else {
			// non-null value, check if we can fit it within the block
			// we also always store strings that have a size >= STRING_BLOCK_LIMIT in the overflow blocks
			index_t string_length = strlen(ldata[i]);
			index_t total_length = string_length + 1 + sizeof(uint16_t);

			if (string_length > stats.max_string_length) {
				stats.max_string_length = string_length;
			}
			if (total_length >= STRING_BLOCK_LIMIT || total_length > BLOCK_SIZE - dictionary_offset - max_vector_count * vector_size) {
				// string is too big for block: write to overflow blocks
				block_id_t block;
				int32_t offset;
				// write the string into the current string block
				WriteString(string_t(ldata[i], string_length + 1), block, offset);

				dictionary_offset += BIG_STRING_MARKER_SIZE;
				auto dict_pos = end - dictionary_offset;

				// write a big string marker into the dictionary
				WriteStringMarker(dict_pos, block, offset);
			} else {
				// string fits in block, append to dictionary and increment dictionary position
				assert(string_length < std::numeric_limits<uint16_t>::max());
				dictionary_offset += total_length;
				auto dict_pos = end - dictionary_offset;

				// first write the length as u16
				uint16_t string_length_u16 = string_length;
				memcpy(dict_pos, &string_length_u16, sizeof(uint16_t));
				// now write the actual string data into the dictionary
				memcpy(dict_pos + sizeof(uint16_t), ldata[i], string_length + 1);
			}
			// place the dictionary offset into the set of vectors
			result_data[k - offset + target_offset] = dictionary_offset;
		}
	}, offset);
}

void StringSegment::WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) {
	uint32_t total_length = string.length + 1 + sizeof(uint32_t);
	unique_ptr<ManagedBufferHandle> handle;
	// check if the string fits in the current block
	if (!head || head->offset + total_length >= head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		index_t alloc_size = std::max((index_t) total_length, (index_t) BLOCK_SIZE);
		auto new_block = make_unique<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		handle = manager.Allocate(alloc_size);
		new_block->block_id = handle->block_id;
		new_block->next = move(head);
		head = move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = manager.PinBuffer(head->block_id);
	}

	result_block = head->block_id;
	result_offset = head->offset;

	// copy the string and the length there
	auto ptr = handle->buffer->data.get() + head->offset;
	memcpy(ptr, &string.length, sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(ptr, string.data, string.length + 1);
	head->offset += total_length;
}

string_t StringSegment::ReadString(TransientScanState &state, block_id_t block, int32_t offset) {
	// now pin the handle, if it is not pinned yet
	ManagedBufferHandle *handle;
	auto entry = state.handles.find(block);
	if (entry == state.handles.end()) {
		auto pinned_handle = manager.PinBuffer(block);
		handle = pinned_handle.get();

		state.handles.insert(make_pair(block, move(pinned_handle)));
	} else {
		handle = (ManagedBufferHandle*) entry->second.get();
	}
	return ReadString(handle->buffer->data.get(), offset);
}

string_t StringSegment::ReadString(data_ptr_t target, int32_t offset) {
	auto ptr = target + offset;
	string_t result;
	result.length = *((uint32_t*) ptr);
	result.data = (char*) (ptr + sizeof(uint32_t));
	return result;
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

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
string_update_info_t StringSegment::CreateStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, index_t vector_offset) {
	// first figure out how big the block needs to be by iterating over the updates
	int32_t lengths[STANDARD_VECTOR_SIZE];
	index_t total_length = 0;
	auto strings = (char**) update.data;
	for(index_t i = 0; i < update.count; i++) {
		if (!update.nullmask[i]) {
			lengths[i] = strlen(strings[i]) + 1;
			total_length += lengths[i];
		} else {
			lengths[i] = 0;
		}
	}

	auto info = make_unique<StringUpdateInfo>();
	info->count = update.count;
	for(index_t i = 0; i < update.count; i++) {
		info->ids[i] = ids[i] - vector_offset;
		info->nullmask[info->ids[i]] = update.nullmask[i];
		// copy the string into the block
		if (lengths[i] > 0) {
			WriteString(string_t(strings[i], lengths[i]), info->block_ids[i], info->offsets[i]);
		} else {
			info->block_ids[i] = INVALID_BLOCK;
			info->offsets[i] = 0;
		}
	}
	return info;
}

void StringSegment::Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) {
	if (!string_updates) {
		string_updates = unique_ptr<string_update_info_t[]>(new string_update_info_t[max_vector_count]);
	}

	// move the original strings into the undo buffer
	// first fetch the strings
	Vector original_data;
	original_data.Initialize(TypeId::VARCHAR);

	FetchBaseData(ids, vector_index, vector_offset, update.count, original_data);
	assert(update.count == original_data.count);

	// now copy them into the undo buffer
	auto strings = (char**) original_data.data;
	for(index_t i = 0; i < original_data.count; i++) {
		strings[i] = (char*) transaction.PushString(string_t(strings[i], strlen(strings[i])));
	}

	string_update_info_t new_update_info;
	// next up: create the updates
	if (!string_updates[vector_index]) {
		// no string updates yet, allocate a block and place the updates there
		new_update_info = CreateStringUpdate(stats, update, ids, vector_offset);
	} else {
		throw Exception("FIXME: merge string update");
	}

	// now that the original strings are placed in the undo buffer and the updated strings are placed in the base table
	// create the update node
	if (!node) {
		// create a new node in the undo buffer for this update
		node = CreateUpdateInfo(transaction, ids, update.count, vector_index, vector_offset, sizeof(char*));

		// copy the nullmask and data into the UpdateInfo
		node->nullmask = original_data.nullmask;
		memcpy(node->tuple_data, strings, sizeof(char*) * original_data.count);
	} else {
		throw Exception("FIXME: merge update");
	}
	// finally move the string updates in place
	string_updates[vector_index] = move(new_update_info);
}

void StringSegment::RollbackUpdate(UpdateInfo *info) {
	throw Exception("FIXME");
}
