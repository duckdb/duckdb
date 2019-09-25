#include "storage/uncompressed_segment.hpp"
#include "storage/buffer_manager.hpp"
#include "common/types/vector.hpp"
#include "storage/table/append_state.hpp"
#include "transaction/version_info.hpp"
#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

static UncompressedSegment::append_function_t GetAppendFunction(TypeId type);
static UncompressedSegment::update_function_t GetUpdateFunction(TypeId type);
static UncompressedSegment::base_table_fetch_function_t GetBaseTableFetchFunction(TypeId type);
static UncompressedSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(TypeId type);
static UncompressedSegment::rollback_update_function_t GetRollbackUpdateFunction(TypeId type);


UncompressedSegment::UncompressedSegment(BufferManager &manager, TypeId type) :
	manager(manager), type(type), block_id(INVALID_BLOCK), dirty(true), tuple_count(0) {
	// transient segment
	// figure out how many vectors we want to store in this block
	this->append_function = GetAppendFunction(type);
	this->update_function = GetUpdateFunction(type);
	this->fetch_from_base_table = GetBaseTableFetchFunction(type);
	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
	this->rollback_update = GetRollbackUpdateFunction(type);

	this->type_size = GetTypeIdSize(type);
	this->vector_size = sizeof(nullmask_t) + type_size * STANDARD_VECTOR_SIZE;
	this->max_vector_count = (BLOCK_SIZE / vector_size) + 1;
	// allocate space for the vectors
	auto block = manager.Allocate(max_vector_count * vector_size);
	this->block_id = block->block_id;
	// initialize nullmasks to 0 for all vectors
	for(index_t i = 0; i < max_vector_count; i++) {
		auto mask = (nullmask_t*) (block->buffer->data.get() + (i * vector_size));
		mask->reset();
	}
	this->versions = nullptr;
}


void UncompressedSegment::InitializeScan(UncompressedSegmentScanState &state) {
	// obtain a read lock and pin the buffer for this segment
	state.handle = manager.PinBuffer(block_id);
}

void UncompressedSegment::Scan(Transaction &transaction, UncompressedSegmentScanState &state, index_t vector_index, Vector &result) {
	auto handle = lock.GetSharedLock();

	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	auto data = state.handle->buffer->data.get();

	auto offset = vector_index * vector_size;

	index_t count = std::min((index_t) STANDARD_VECTOR_SIZE, tuple_count - vector_index * STANDARD_VECTOR_SIZE);
	if (!versions || !versions[vector_index]) {
		// no version information: simply set up the data pointer and read the nullmask
		result.nullmask = *((nullmask_t*) (data + offset));
		memcpy(result.data, data + offset + sizeof(nullmask_t), count * type_size);
		result.count = count;
	} else {
		// version information: need to follow version chain to figure out which versions to use for each tuple
		UpdateInfo *info[STANDARD_VECTOR_SIZE];
		sel_t remaining[STANDARD_VECTOR_SIZE];
		for(index_t i = 0; i < count; i++) {
			info[i] = nullptr;
			remaining[i] = i;
		}
		// follow the version chain
		UpdateInfo *current = versions[vector_index];
		while(current) {
			if (current->version_number > transaction.start_time && current->version_number != transaction.transaction_id) {
				// these tuples were either committed AFTER this transaction started or are not committed yet, use tuples stored in this version
				for(index_t i = 0; i < current->N; i++) {
					info[current->tuples[i]] = current;
				}
			}
			current = current->next;
		}
		// now fetch the entries from the different UpdateInfo's we have collected
		index_t remaining_count = count;
		while(remaining_count > 0) {
			sel_t current_tuples[STANDARD_VECTOR_SIZE];
			index_t current_count = 1, next_remaining_count = 0;

			// select the first UpdateInfo, and select all entries that belong together with that UpdateInfo
			current = info[remaining[0]];
			current_tuples[0] = remaining[0];
			for(index_t i = 1; i < remaining_count; i++) {
				if (info[remaining[i]] == current) {
					current_tuples[current_count++] = remaining[i];
				} else {
					remaining[next_remaining_count++] = remaining[i];
				}
			}
			// now fetch the tuples from the current update info and place them in the result vector
			if (!current) {
				// fetch from base table
				fetch_from_base_table(data + offset, current_tuples, current_count, result);
			} else {
				// fetch from update info
				fetch_from_update_info(current, current_tuples, current_count, result);
			}
			remaining_count = next_remaining_count;
		}
		result.count = count;
	}
}

index_t UncompressedSegment::Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count) {
	assert(data.type == type);

	index_t initial_count = tuple_count;
	while(count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		index_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			break;
		}
		index_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		index_t append_count = std::min(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		append_function(stats, state.handle->buffer->data.get() + vector_size * vector_index, current_tuple_count, data, offset, append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

static void CheckForConflicts(UpdateInfo *info, Transaction &transaction, Vector &update, row_t *ids, row_t offset, UpdateInfo *& node) {
	if (info->version_number == transaction.transaction_id) {
		// this UpdateInfo belongs to the current transaction, set it in the node
		node = info;
	} else if (info->version_number > transaction.start_time) {
		// potential conflict, check that tuple ids do not conflict
		// as both ids and info->tuples are sorted, this is similar to a merge join
		index_t i = 0, j = 0;
		while(true) {
			auto id = ids[i] - offset;
			if (id == info->tuples[j]) {
				throw Exception("Conflict on update!");
			} else if (id < info->tuples[j]) {
				// id < the current tuple in info, move to next id
				i++;
				if (i == update.count) {
					break;
				}
			} else {
				// id > the current tuple, move to next tuple in info
				j++;
				if (j == info->N) {
					break;
				}
			}
		}
	}
	if (info->next) {
		CheckForConflicts(info->next, transaction, update, ids, offset, node);
	}
}

void UncompressedSegment::Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, row_t offset) {
	assert(!update.sel_vector);

	// obtain an exclusive lock
	auto handle = lock.GetExclusiveLock();

	// create the versions for this segment, if there are none yet
	if (!versions) {
		this->versions = unique_ptr<UpdateInfo*[]>(new UpdateInfo*[max_vector_count]);
		for(index_t i = 0; i < max_vector_count; i++) {
			this->versions[i] = nullptr;
		}
	}

	// get the vector index based on the first id
	// we assert that all updates must be part of the same vector
	auto first_id = update.sel_vector ? ids[update.sel_vector[0]] : ids[0];
	index_t vector_index = (first_id - offset) / STANDARD_VECTOR_SIZE;
	index_t vector_offset = offset + vector_index * STANDARD_VECTOR_SIZE;

	assert(first_id >= offset);
	assert(vector_index < max_vector_count);

	// first check the version chain
	UpdateInfo *node = nullptr;
	if (versions[vector_index]) {
		// there is already a version here, check if there are any conflicts and search for the node that belongs to this transaction in the version chain
		CheckForConflicts(versions[vector_index], transaction, update, ids, vector_offset, node);
	}
	if (!node) {
		// create a new node in the undo buffer for this update
		node = transaction.CreateUpdateInfo(type_size, STANDARD_VECTOR_SIZE);
		node->segment = this;
		node->vector_index = vector_index;
		node->prev = nullptr;
		node->next = versions[vector_index];
		if (node->next) {
			node->next->prev = node;
		}
		versions[vector_index] = node;

		// set up the tuple ids
		node->N = update.count;
		VectorOperations::Exec(update, [&](index_t i, index_t k) {
			assert((index_t) ids[i] >= vector_offset && (index_t) ids[i] < vector_offset + STANDARD_VECTOR_SIZE);
			node->tuples[k] = ids[i] - vector_offset;
		});
		// now move the original data into the UpdateInfo
		auto handle = manager.PinBuffer(block_id);
		update_function(stats, node, handle->buffer->data.get() + vector_index * vector_size, update);
	} else {
		throw Exception("FIXME: update existing node");
	}
}

void UncompressedSegment::RollbackUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();
	auto handle = manager.PinBuffer(block_id);

	// move the data from the UpdateInfo back into the base table
	rollback_update(info, handle->buffer->data.get() + info->vector_index * vector_size);

	CleanupUpdate(info);
}

void UncompressedSegment::CleanupUpdate(UpdateInfo *info) {
	if (info->prev) {
		// there is a prev info: remove from the chain
		auto prev = info->prev;
		prev->next = info->next;
		if (prev->next) {
			prev->next->prev = prev;
		}
	} else {
		// there is no prev info: remove from base segment
		info->segment->versions[info->vector_index] = info->next;
		if (info->next) {
			info->next->prev = nullptr;
		}
	}
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T> static void update_min_max(T value, T *__restrict min, T *__restrict max) {
	if (value < *min) {
		*min = value;
	}
	if (value > *max) {
		*max = value;
	}
}

template <class T>
static void append_loop_null(T *__restrict source, nullmask_t &result_nullmask, T *__restrict target, index_t target_offset, index_t offset, index_t count,
                             sel_t *__restrict sel_vector, nullmask_t &nullmask, T *__restrict min, T *__restrict max,
                             bool &has_null) {
	// null values, have to check the NULL values in the mask
	VectorOperations::Exec(
	    sel_vector, count + offset,
	    [&](index_t i, index_t k) {
		    if (nullmask[i]) {
				result_nullmask[k - offset + target_offset] = true;
			    has_null = true;
		    } else {
			    update_min_max(source[i], min, max);
			    target[k - offset + target_offset] = source[i];
		    }
	    },
	    offset);
}

template <class T>
static void append_loop_no_null(T *__restrict source, T *__restrict target, index_t target_offset, index_t offset, index_t count,
                                sel_t *__restrict sel_vector, T *__restrict min, T *__restrict max) {
	VectorOperations::Exec(
	    sel_vector, count + offset,
	    [&](index_t i, index_t k) {
		    update_min_max(source[i], min, max);
		    target[k - offset + target_offset] = source[i];
	    },
	    offset);
}

template <class T>
static void append_loop(SegmentStatistics &stats, data_ptr_t target, index_t target_offset, Vector &source, index_t offset, index_t count) {
	assert(offset + count <= source.count);
	auto ldata = (T *)source.data;
	auto result_data = (T *) (target + sizeof(nullmask_t));
	auto nullmask = (nullmask_t*) target;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	if (source.nullmask.any()) {
		append_loop_null<T>(ldata, *nullmask, result_data, target_offset, offset, count, source.sel_vector, source.nullmask, min, max,
		                    stats.has_null);
	} else {
		append_loop_no_null<T>(ldata, result_data, target_offset, offset, count, source.sel_vector, min, max);
	}
}

static UncompressedSegment::append_function_t GetAppendFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return append_loop<int8_t>;
	case TypeId::SMALLINT:
		return append_loop<int16_t>;
	case TypeId::INTEGER:
		return append_loop<int32_t>;
	case TypeId::BIGINT:
		return append_loop<int64_t>;
	case TypeId::FLOAT:
		return append_loop<float>;
	case TypeId::DOUBLE:
		return append_loop<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
template<class T>
static void update_loop_null(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data,
                             nullmask_t &undo_nullmask, nullmask_t &base_nullmask, nullmask_t &new_nullmask, index_t count,
                                sel_t *__restrict base_sel, T *__restrict min, T *__restrict max) {
	for(index_t i = 0; i < count; i++) {
		// first move the base data into the undo buffer info
		undo_data[i] = base_data[base_sel[i]];
		undo_nullmask[base_sel[i]] = base_nullmask[base_sel[i]];
		// now move the new data in-place into the base table
		base_data[base_sel[i]] = new_data[i];
		base_nullmask[base_sel[i]] = new_nullmask[i];
		// update the min max with the new data
		update_min_max(new_data[i], min, max);
	}
}

template<class T>
static void update_loop_no_null(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data, index_t count,
                                sel_t *__restrict base_sel, T *__restrict min, T *__restrict max) {
	for(index_t i = 0; i < count; i++) {
		// first move the base data into the undo buffer info
		undo_data[i] = base_data[base_sel[i]];
		// now move the new data in-place into the base table
		base_data[base_sel[i]] = new_data[i];
		// update the min max with the new data
		update_min_max(new_data[i], min, max);
	}
}

template <class T>
static void update_loop(SegmentStatistics &stats, UpdateInfo *info, data_ptr_t base, Vector &update) {
	auto update_data = (T*) update.data;
	auto nullmask = (nullmask_t *) base;
	auto base_data = (T*) (base + sizeof(nullmask_t));
	auto undo_data = (T*) info->tuple_data;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();

	if (update.nullmask.any() || nullmask->any()) {
		update_loop_null(undo_data, base_data, update_data, info->nullmask, *nullmask, update.nullmask, info->N, info->tuples, min, max);
	} else {
		update_loop_no_null(undo_data, base_data, update_data, info->N, info->tuples, min, max);
	}
}

static UncompressedSegment::update_function_t GetUpdateFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return update_loop<int8_t>;
	case TypeId::SMALLINT:
		return update_loop<int16_t>;
	case TypeId::INTEGER:
		return update_loop<int32_t>;
	case TypeId::BIGINT:
		return update_loop<int64_t>;
	case TypeId::FLOAT:
		return update_loop<float>;
	case TypeId::DOUBLE:
		return update_loop<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update Fetch
//===--------------------------------------------------------------------===//
template <class T>
static void base_table_fetch_loop(T *__restrict base_data, T *__restrict result_data, nullmask_t &base_nullmask, nullmask_t &result_nullmask, sel_t tuples[], sel_t count) {
	if (base_nullmask.any()) {
		for(index_t i = 0; i < count; i++) {
			result_nullmask[tuples[i]] = base_nullmask[tuples[i]];
			result_data[tuples[i]] = base_data[tuples[i]];
		}
	} else {
		for(index_t i = 0; i < count; i++) {
			result_data[tuples[i]] = base_data[tuples[i]];
		}
	}
}

template <class T>
static void base_table_fetch(data_ptr_t base, sel_t tuples[], sel_t count, Vector &result) {
	auto nullmask = (nullmask_t *) base;
	auto base_data = (T*) (base + sizeof(nullmask_t));
	auto result_data = (T*) result.data;

	base_table_fetch_loop(base_data, result_data, *nullmask, result.nullmask, tuples, count);
}

static UncompressedSegment::base_table_fetch_function_t GetBaseTableFetchFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return base_table_fetch<int8_t>;
	case TypeId::SMALLINT:
		return base_table_fetch<int16_t>;
	case TypeId::INTEGER:
		return base_table_fetch<int32_t>;
	case TypeId::BIGINT:
		return base_table_fetch<int64_t>;
	case TypeId::FLOAT:
		return base_table_fetch<float>;
	case TypeId::DOUBLE:
		return base_table_fetch<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

template <class T>
static void update_info_fetch_loop(UpdateInfo &info, T *__restrict info_data, T *__restrict result_data, nullmask_t &info_nullmask, nullmask_t &result_nullmask, sel_t tuples[], sel_t count) {
	index_t idx = 0;
	for(index_t i = 0; i < info.N; i++) {
		if (info.tuples[i] == tuples[idx]) {
			result_data[tuples[idx]] = info_data[i];
			result_nullmask[tuples[idx]] = info_nullmask[tuples[idx]];
			idx++;
			if (idx == count) {
				break;
			}
		}
	}
	assert(idx == count);
}

template <class T>
static void update_info_fetch(UpdateInfo *info, sel_t tuples[], sel_t count, Vector &result) {
	auto info_data = (T*) info->tuple_data;
	auto result_data = (T*) result.data;

	update_info_fetch_loop(*info, info_data, result_data, info->nullmask, result.nullmask, tuples, count);
}

static UncompressedSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return update_info_fetch<int8_t>;
	case TypeId::SMALLINT:
		return update_info_fetch<int16_t>;
	case TypeId::INTEGER:
		return update_info_fetch<int32_t>;
	case TypeId::BIGINT:
		return update_info_fetch<int64_t>;
	case TypeId::FLOAT:
		return update_info_fetch<float>;
	case TypeId::DOUBLE:
		return update_info_fetch<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Rollback Update
//===--------------------------------------------------------------------===//
template <class T>
static void rollback_update(UpdateInfo *info, data_ptr_t base) {
	auto &nullmask = *((nullmask_t *) base);
	auto info_data = (T*) info->tuple_data;
	auto base_data = (T*) (base + sizeof(nullmask_t));

	for(index_t i = 0; i < info->N; i++) {
		base_data[info->tuples[i]] = info_data[i];
		nullmask[info->tuples[i]] = info->nullmask[info->tuples[i]];
	}
}

static UncompressedSegment::rollback_update_function_t GetRollbackUpdateFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return rollback_update<int8_t>;
	case TypeId::SMALLINT:
		return rollback_update<int16_t>;
	case TypeId::INTEGER:
		return rollback_update<int32_t>;
	case TypeId::BIGINT:
		return rollback_update<int64_t>;
	case TypeId::FLOAT:
		return rollback_update<float>;
	case TypeId::DOUBLE:
		return rollback_update<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}

}
