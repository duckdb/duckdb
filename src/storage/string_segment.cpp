#include "storage/string_segment.hpp"
#include "storage/buffer_manager.hpp"
// #include "common/types/vector.hpp"
// #include "storage/table/append_state.hpp"
#include "transaction/update_info.hpp"
// #include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

static bool IsValidStringLocation(string_location_t location) {
	return location.offset < BLOCK_SIZE && (location.block_id == INVALID_BLOCK || location.block_id >= MAXIMUM_BLOCK);
}

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

//===--------------------------------------------------------------------===//
// Fetch base data
//===--------------------------------------------------------------------===//
index_t StringSegment::FetchBaseData(TransientScanState &state, index_t vector_index, Vector &result) {
	// clear any previously locked buffers and get the primary buffer handle
	auto handle = (ManagedBufferHandle*) state.primary_handle.get();
	state.handles.clear();

	index_t count = GetVectorCount(vector_index);

	// fetch the data from the base segment
	FetchBaseData(state, handle->buffer->data.get(), vector_index, result, count);
	return count;
}

//===--------------------------------------------------------------------===//
// Fetch update data
//===--------------------------------------------------------------------===//
void StringSegment::FetchUpdateData(TransientScanState &state, Transaction &transaction, UpdateInfo *current, Vector &result, index_t count) {
	// fetch data from updates
	auto handle = (ManagedBufferHandle*) state.primary_handle.get();

	auto result_data = (char**) result.data;
	while(current) {
		if (current->version_number > transaction.start_time && current->version_number != transaction.transaction_id) {
			// these tuples were either committed AFTER this transaction started or are not committed yet, use tuples stored in this version
			auto info_data = (string_location_t*) current->tuple_data;
			for(index_t i = 0; i < current->N; i++) {
				auto string = FetchString(state.handles, handle->buffer->data.get(), info_data[i]);
				result_data[current->tuples[i]] = string.data;
				result.nullmask[current->tuples[i]] = current->nullmask[current->tuples[i]];
			}
		}
		current = current->next;
	}
}

void StringSegment::FetchStringLocations(data_ptr_t baseptr, row_t *ids, index_t vector_index, index_t vector_offset, index_t count, string_location_t result[]) {
	auto base = baseptr + vector_index * vector_size;
	auto base_data = (int32_t *) (base + sizeof(nullmask_t));

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
				result[i].block_id = info.block_ids[update_idx];
				result[i].offset = info.offsets[update_idx];
				update_idx++;
			} else {
				// use base table info
				result[i] = FetchStringLocation(baseptr, base_data[id]);
			}
		}
	} else {
		// no updates: fetch strings from base vector
		for(index_t i = 0; i < count; i++) {
			auto id = ids[i] - vector_offset;
			result[i] = FetchStringLocation(baseptr, base_data[id]);
		}
	}
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
				result_data[i] = ReadString(state.handles, info.block_ids[update_idx], info.offsets[update_idx]).data;
				update_idx++;
			} else {
				// use base table info
				result_data[i] = FetchStringFromDict(state.handles, baseptr, base_data[i]).data;
			}
		}
	} else {
		// no updates: fetch only from the string dictionary
		for(index_t i = 0; i < count; i++) {
			result_data[i] = FetchStringFromDict(state.handles, baseptr, base_data[i]).data;
		}
	}
	result.nullmask = base_nullmask;
	result.count = count;
}

string_location_t StringSegment::FetchStringLocation(data_ptr_t baseptr, int32_t dict_offset) {
	if (dict_offset == 0) {
		return string_location_t(INVALID_BLOCK, 0);
	}
	// look up result in dictionary
	auto dict_end = baseptr + BLOCK_SIZE;
	auto dict_pos = dict_end - dict_offset;
	auto string_length = *((uint16_t*) dict_pos);
	string_location_t result;
	if (string_length == BIG_STRING_MARKER) {
		ReadStringMarker(dict_pos, result.block_id, result.offset);
	} else {
		result.block_id = INVALID_BLOCK;
		result.offset = dict_offset;
	}
	return result;
}

string_t StringSegment::FetchStringFromDict(buffer_handle_set_t &handles, data_ptr_t baseptr, int32_t dict_offset) {
	// fetch base data
	string_location_t location = FetchStringLocation(baseptr, dict_offset);
	return FetchString(handles, baseptr, location);
}

string_t StringSegment::FetchString(buffer_handle_set_t &handles, data_ptr_t baseptr, string_location_t location) {
	if (location.block_id != INVALID_BLOCK) {
		// big string marker: read from separate block
		return ReadString(handles, location.block_id, location.offset);
	} else {
		if (location.offset == 0) {
			return string_t(nullptr, 0);
		}
		// normal string: read string from this block
		auto dict_end = baseptr + BLOCK_SIZE;
		auto dict_pos = dict_end - location.offset;
		auto string_length = *((uint16_t*) dict_pos);

		string_t result;
		result.length = string_length;
		result.data = (char*) (dict_pos + sizeof(uint16_t));
		return result;
	}

}

void StringSegment::FetchRow(FetchState &state, Transaction &transaction, row_t row_id, Vector &result) {
	auto read_lock = lock.GetSharedLock();

	index_t vector_index = row_id / STANDARD_VECTOR_SIZE;
	index_t id_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;
	assert(vector_index < max_vector_count);

	// fetch a single row from the string segment
	auto handle = manager.PinBuffer(block_id);

	auto baseptr = handle->buffer->data.get();
	auto base = baseptr + vector_index * vector_size;
	auto &base_nullmask = *((nullmask_t*) base);
	auto base_data = (int32_t *) (base + sizeof(nullmask_t));
	auto result_data = (char**) result.data;

	result_data[result.count] = nullptr;
	// first see if there is any updated version of this tuple we must fetch
	if (versions && versions[vector_index]) {
		auto current = versions[vector_index];
		while(current) {
			if (current->version_number > transaction.start_time && current->version_number != transaction.transaction_id) {
				// these tuples were either committed AFTER this transaction started or are not committed yet, use tuples stored in this version
				auto info_data = (string_location_t*) current->tuple_data;
				for(index_t i = 0; i < current->N; i++) {
					if (current->tuples[i] == id_in_vector) {
						auto string = FetchString(state.handles, handle->buffer->data.get(), info_data[i]);
						result_data[result.count] = string.data;
						result.nullmask[result.count] = current->nullmask[current->tuples[i]];
					}
				}
			}
			current = current->next;
		}
	}
	if (!result_data[result.count]) {
		// there was no updated version to be fetched: fetch the base version instead
		if (string_updates && string_updates[vector_index]) {
			// there are updates: check if we should use them
			auto &info = *string_updates[vector_index];
			for(index_t i = 0; i < info.count; i++) {
				if (info.ids[i] == id_in_vector) {
					// use the update
					result_data[result.count] = ReadString(state.handles, info.block_ids[i], info.offsets[i]).data;
					break;
				} else if (info.ids[i] > id_in_vector) {
					break;
				}
			}
		} else {
			// no version was found yet: fetch base table version
			result_data[result.count] = FetchStringFromDict(state.handles, baseptr, base_data[id_in_vector]).data;
		}
	}
	result.nullmask[result.count] = base_nullmask[id_in_vector];
	result.count++;

	state.handles[block_id] = move(handle);
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
				WriteString(string_t(ldata[i], string_length), block, offset);

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
	assert(strlen(string.data) == string.length);
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

string_t StringSegment::ReadString(buffer_handle_set_t &handles, block_id_t block, int32_t offset) {
	assert(offset < BLOCK_SIZE);
	if (block == INVALID_BLOCK) {
		return string_t(nullptr, 0);
	}
	// now pin the handle, if it is not pinned yet
	ManagedBufferHandle *handle;
	auto entry = handles.find(block);
	if (entry == handles.end()) {
		auto pinned_handle = manager.PinBuffer(block);
		handle = pinned_handle.get();

		handles.insert(make_pair(block, move(pinned_handle)));
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
// String Update
//===--------------------------------------------------------------------===//
string_update_info_t StringSegment::CreateStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, index_t vector_offset) {
	auto info = make_unique<StringUpdateInfo>();
	info->count = update.count;
	auto strings = (char**) update.data;
	for(index_t i = 0; i < update.count; i++) {
		info->ids[i] = ids[i] - vector_offset;
		// copy the string into the block
		if (!update.nullmask[i]) {
			WriteString(string_t(strings[i], strlen(strings[i])), info->block_ids[i], info->offsets[i]);
		} else {
			info->block_ids[i] = INVALID_BLOCK;
			info->offsets[i] = 0;
		}
	}
	return info;
}

string_update_info_t StringSegment::MergeStringUpdate(SegmentStatistics &stats, Vector &update, row_t *ids, index_t vector_offset, StringUpdateInfo &update_info) {
	auto info = make_unique<StringUpdateInfo>();

	// perform a merge between the new and old indexes
	auto strings = (char**) update.data;
	auto pick_new = [&](index_t id, index_t idx, index_t count) {
		info->ids[count] = id;
		if (!update.nullmask[idx]) {
			WriteString(string_t(strings[idx], strlen(strings[idx])), info->block_ids[count], info->offsets[count]);
		} else {
			info->block_ids[count] = INVALID_BLOCK;
			info->offsets[count] = 0;
		}

	};
	auto merge = [&](index_t id, index_t aidx, index_t bidx, index_t count) {
		// merge: only pick new entry
		pick_new(id, aidx, count);
	};
	auto pick_old = [&](index_t id, index_t bidx, index_t count) {
		// pick old entry
		info->ids[count] = id;
		info->block_ids[count] = update_info.block_ids[bidx];
		info->offsets[count] = update_info.offsets[bidx];
	};

	info->count = merge_loop(ids, update_info.ids, update.count, update_info.count, vector_offset, merge, pick_new, pick_old);
	return info;
}

//===--------------------------------------------------------------------===//
// Update Info
//===--------------------------------------------------------------------===//
void StringSegment::MergeUpdateInfo(UpdateInfo *node, Vector &update, row_t *ids, index_t vector_offset, string_location_t base_data[], nullmask_t base_nullmask) {
	auto info_data = (string_location_t*) node->tuple_data;

	// first we copy the old update info into a temporary structure
	sel_t old_ids[STANDARD_VECTOR_SIZE];
	string_location_t old_data[STANDARD_VECTOR_SIZE];
	string_location_t stored_data[STANDARD_VECTOR_SIZE];

	memcpy(old_ids, node->tuples, node->N * sizeof(sel_t));
	memcpy(old_data, node->tuple_data, node->N * sizeof(string_location_t));

	// now we perform a merge of the new ids with the old ids
	auto merge = [&](index_t id, index_t aidx, index_t bidx, index_t count) {
		// new_id and old_id are the same, insert the old data in the UpdateInfo
		assert(IsValidStringLocation(old_data[bidx]));
		info_data[count] = old_data[bidx];
		node->tuples[count] = id;
	};
	auto pick_new = [&](index_t id, index_t aidx, index_t count) {
		// new_id comes before the old id, insert the base table data into the update info
		assert(IsValidStringLocation(base_data[aidx]));
		info_data[count] = base_data[aidx];
		node->nullmask[id] = base_nullmask[aidx];

		node->tuples[count] = id;
	};
	auto pick_old = [&](index_t id, index_t bidx, index_t count) {
		// old_id comes before new_id, insert the old data
		assert(IsValidStringLocation(old_data[bidx]));
		info_data[count] = old_data[bidx];
		node->tuples[count] = id;
	};
	// perform the merge
	node->N = merge_loop(ids, old_ids, update.count, node->N, vector_offset, merge, pick_new, pick_old);
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void StringSegment::Update(DataTable &table, SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) {
	if (!string_updates) {
		string_updates = unique_ptr<string_update_info_t[]>(new string_update_info_t[max_vector_count]);
	}

	// first pin the base block
	auto handle = manager.PinBuffer(block_id);
	auto baseptr = handle->buffer->data.get();
	auto base = baseptr + vector_index * vector_size;
	auto &base_nullmask = *((nullmask_t*) base);

	// fetch the original string locations and copy the original nullmask
	string_location_t string_locations[STANDARD_VECTOR_SIZE];
	nullmask_t original_nullmask = base_nullmask;
	FetchStringLocations(baseptr, ids, vector_index, vector_offset, update.count, string_locations);

	string_update_info_t new_update_info;
	// next up: create the updates
	if (!string_updates[vector_index]) {
		// no string updates yet, allocate a block and place the updates there
		new_update_info = CreateStringUpdate(stats, update, ids, vector_offset);
	} else {
		// string updates already exist, merge the string updates together
		new_update_info = MergeStringUpdate(stats, update, ids, vector_offset, *string_updates[vector_index]);
	}

	// now update the original nullmask
	for(index_t i = 0; i < update.count; i++) {
		base_nullmask[ids[i] - vector_offset] = update.nullmask[i];
	}

	// now that the original strings are placed in the undo buffer and the updated strings are placed in the base table
	// create the update node
	if (!node) {
		// create a new node in the undo buffer for this update
		node = CreateUpdateInfo(table, transaction, ids, update.count, vector_index, vector_offset, sizeof(string_location_t));

		// copy the string location data into the undo buffer
		node->nullmask = original_nullmask;
		memcpy(node->tuple_data, string_locations, sizeof(string_location_t) * update.count);
	} else {
		// node in the update info already exists, merge the new updates in
		MergeUpdateInfo(node, update, ids, vector_offset, string_locations, original_nullmask);
	}
	// finally move the string updates in place
	string_updates[vector_index] = move(new_update_info);
}

void StringSegment::RollbackUpdate(UpdateInfo *info) {
	auto lock_handle = lock.GetExclusiveLock();

	index_t new_count = 0;
	auto &update_info = *string_updates[info->vector_index];
	auto string_locations = (string_location_t*) info->tuple_data;

	// put the previous NULL values back
	auto handle = manager.PinBuffer(block_id);
	auto baseptr = handle->buffer->data.get();
	auto base = baseptr + info->vector_index * vector_size;
	auto &base_nullmask = *((nullmask_t*) base);
	for(index_t i = 0; i < info->N; i++) {
		base_nullmask[info->tuples[i]] = info->nullmask[info->tuples[i]];
	}

	// now put the original values back into the update info
	index_t old_idx = 0;
	for(index_t i = 0; i < update_info.count; i++) {
		if (old_idx >= info->N || update_info.ids[i] != info->tuples[old_idx]) {
			assert(old_idx >= info->N || update_info.ids[i] < info->tuples[old_idx]);
			// this entry is not rolled back: insert entry directly
			update_info.ids[new_count] = update_info.ids[i];
			update_info.block_ids[new_count] = update_info.block_ids[i];
			update_info.offsets[new_count] = update_info.offsets[i];
			new_count++;
		} else {
			// this entry is being rolled back
			auto &old_location = string_locations[old_idx];
			if (old_location.block_id != INVALID_BLOCK) {
				// not rolled back to base table: insert entry again
				update_info.ids[new_count] = update_info.ids[i];
				update_info.block_ids[new_count] = old_location.block_id;
				update_info.offsets[new_count] = old_location.offset;
				new_count++;
			}
			old_idx++;
		}
	}

	if (new_count == 0) {
		// all updates are rolled back: delete the string update vector
		string_updates[info->vector_index].reset();
	} else {
		// set the count of the new string update vector
		update_info.count = new_count;
	}
	CleanupUpdate(info);
}
