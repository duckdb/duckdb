#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static NumericSegment::append_function_t GetAppendFunction(TypeId type);
static NumericSegment::update_function_t GetUpdateFunction(TypeId type);
static NumericSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(TypeId type);
static NumericSegment::rollback_update_function_t GetRollbackUpdateFunction(TypeId type);
static NumericSegment::merge_update_function_t GetMergeUpdateFunction(TypeId type);
static NumericSegment::update_info_append_function_t GetUpdateInfoAppendFunction(TypeId type);

NumericSegment::NumericSegment(BufferManager &manager, TypeId type, index_t row_start, block_id_t block)
    : UncompressedSegment(manager, type, row_start) {
	// set up the different functions for this type of segment
	this->append_function = GetAppendFunction(type);
	this->update_function = GetUpdateFunction(type);
	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
	this->append_from_update_info = GetUpdateInfoAppendFunction(type);
	this->rollback_update = GetRollbackUpdateFunction(type);
	this->merge_update_function = GetMergeUpdateFunction(type);

	// figure out how many vectors we want to store in this block
	this->type_size = GetTypeIdSize(type);
	this->vector_size = sizeof(nullmask_t) + type_size * STANDARD_VECTOR_SIZE;
	this->max_vector_count = Storage::BLOCK_SIZE / vector_size;

	this->block_id = block;
	if (block_id == INVALID_BLOCK) {
		// no block id specified: allocate a buffer for the uncompressed segment
		auto handle = manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
		this->block_id = handle->block_id;
		// initialize nullmasks to 0 for all vectors
		for (index_t i = 0; i < max_vector_count; i++) {
			auto mask = (nullmask_t *)(handle->node->buffer + (i * vector_size));
			mask->reset();
		}
	}
}

//===--------------------------------------------------------------------===//
// Fetch base data
//===--------------------------------------------------------------------===//
void NumericSegment::FetchBaseData(ColumnScanState &state, index_t vector_index, Vector &result) {
	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	// pin the buffer for this segment
	auto handle = manager.Pin(block_id);
	auto data = handle->node->buffer;

	auto offset = vector_index * vector_size;

	index_t count = GetVectorCount(vector_index);
	// fetch the nullmask and copy the data from the base table
	result.nullmask = *((nullmask_t *)(data + offset));
	memcpy(result.GetData(), data + offset + sizeof(nullmask_t), count * type_size);
	result.SetCount(count);
}

void NumericSegment::FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *version,
                                     Vector &result) {
	fetch_from_update_info(transaction, version, result);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void NumericSegment::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result) {
	auto read_lock = lock.GetSharedLock();
	auto handle = manager.Pin(block_id);

	// get the vector index
	index_t vector_index = row_id / STANDARD_VECTOR_SIZE;
	index_t id_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;
	assert(vector_index < max_vector_count);

	// first fetch the data from the base table
	auto data = handle->node->buffer + vector_index * vector_size;
	auto &nullmask = *((nullmask_t *)(data));
	auto vector_ptr = data + sizeof(nullmask_t);

	result.nullmask[result.size()] = nullmask[id_in_vector];
	memcpy(result.GetData() + result.size() * type_size, vector_ptr + id_in_vector * type_size, type_size);
	if (versions && versions[vector_index]) {
		// version information: follow the version chain to find out if we need to load this tuple data from any other
		// version
		append_from_update_info(transaction, versions[vector_index], id_in_vector, result);
	}
	result.SetCount(result.size() + 1);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
index_t NumericSegment::Append(SegmentStatistics &stats, Vector &data, index_t offset, index_t count) {
	assert(data.type == type);
	auto handle = manager.Pin(block_id);

	index_t initial_count = tuple_count;
	while (count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		index_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			break;
		}
		index_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		index_t append_count = std::min(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		append_function(stats, handle->node->buffer + vector_size * vector_index, current_tuple_count, data, offset,
		                append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void NumericSegment::Update(ColumnData &column_data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
                            row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) {
	if (!node) {
		auto handle = manager.Pin(block_id);

		// create a new node in the undo buffer for this update
		node = CreateUpdateInfo(column_data, transaction, ids, update.size(), vector_index, vector_offset, type_size);
		// now move the original data into the UpdateInfo
		update_function(stats, node, handle->node->buffer + vector_index * vector_size, update);
	} else {
		// node already exists for this transaction, we need to merge the new updates with the existing updates
		auto handle = manager.Pin(block_id);

		merge_update_function(stats, node, handle->node->buffer + vector_index * vector_size, update, ids,
		                      vector_offset);
	}
}

void NumericSegment::RollbackUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();
	auto handle = manager.Pin(block_id);

	// move the data from the UpdateInfo back into the base table
	rollback_update(info, handle->node->buffer + info->vector_index * vector_size);

	CleanupUpdate(info);
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
static void append_loop_null(T *__restrict source, nullmask_t &result_nullmask, T *__restrict target,
                             index_t target_offset, index_t offset, index_t count, sel_t *__restrict sel_vector,
                             nullmask_t &nullmask, T *__restrict min, T *__restrict max, bool &has_null) {
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
static void append_loop_no_null(T *__restrict source, T *__restrict target, index_t target_offset, index_t offset,
                                index_t count, sel_t *__restrict sel_vector, T *__restrict min, T *__restrict max) {
	VectorOperations::Exec(
	    sel_vector, count + offset,
	    [&](index_t i, index_t k) {
		    update_min_max(source[i], min, max);
		    target[k - offset + target_offset] = source[i];
	    },
	    offset);
}

template <class T>
static void append_loop(SegmentStatistics &stats, data_ptr_t target, index_t target_offset, Vector &source,
                        index_t offset, index_t count) {
	assert(offset + count <= source.size());
	auto ldata = (T *)source.GetData();
	auto result_data = (T *)(target + sizeof(nullmask_t));
	auto nullmask = (nullmask_t *)target;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	if (source.nullmask.any()) {
		append_loop_null<T>(ldata, *nullmask, result_data, target_offset, offset, count, source.sel_vector(),
		                    source.nullmask, min, max, stats.has_null);
	} else {
		append_loop_no_null<T>(ldata, result_data, target_offset, offset, count, source.sel_vector(), min, max);
	}
}

static NumericSegment::append_function_t GetAppendFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return append_loop<int8_t>;
	case TypeId::INT16:
		return append_loop<int16_t>;
	case TypeId::INT32:
		return append_loop<int32_t>;
	case TypeId::INT64:
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
template <class T>
static void update_loop_null(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data,
                             nullmask_t &undo_nullmask, nullmask_t &base_nullmask, nullmask_t &new_nullmask,
                             index_t count, sel_t *__restrict base_sel, T *__restrict min, T *__restrict max) {
	for (index_t i = 0; i < count; i++) {
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

template <class T>
static void update_loop_no_null(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data, index_t count,
                                sel_t *__restrict base_sel, T *__restrict min, T *__restrict max) {
	for (index_t i = 0; i < count; i++) {
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
	auto update_data = (T *)update.GetData();
	auto nullmask = (nullmask_t *)base;
	auto base_data = (T *)(base + sizeof(nullmask_t));
	auto undo_data = (T *)info->tuple_data;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();

	if (update.nullmask.any() || nullmask->any()) {
		update_loop_null(undo_data, base_data, update_data, info->nullmask, *nullmask, update.nullmask, info->N,
		                 info->tuples, min, max);
	} else {
		update_loop_no_null(undo_data, base_data, update_data, info->N, info->tuples, min, max);
	}
}

static NumericSegment::update_function_t GetUpdateFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return update_loop<int8_t>;
	case TypeId::INT16:
		return update_loop<int16_t>;
	case TypeId::INT32:
		return update_loop<int32_t>;
	case TypeId::INT64:
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
// Merge Update
//===--------------------------------------------------------------------===//
template <class T>
static void merge_update_loop(SegmentStatistics &stats, UpdateInfo *node, data_ptr_t base, Vector &update, row_t *ids,
                              index_t vector_offset) {
	auto &base_nullmask = *((nullmask_t *)base);
	auto base_data = (T *)(base + sizeof(nullmask_t));
	auto info_data = (T *)node->tuple_data;
	auto update_data = (T *)update.GetData();

	// first we copy the old update info into a temporary structure
	sel_t old_ids[STANDARD_VECTOR_SIZE];
	T old_data[STANDARD_VECTOR_SIZE];

	memcpy(old_ids, node->tuples, node->N * sizeof(sel_t));
	memcpy(old_data, node->tuple_data, node->N * sizeof(T));

	// now we perform a merge of the new ids with the old ids
	auto merge = [&](index_t id, index_t aidx, index_t bidx, index_t count) {
		// new_id and old_id are the same:
		// insert the new data into the base table
		base_nullmask[id] = update.nullmask[aidx];
		base_data[id] = update_data[aidx];
		// insert the old data in the UpdateInfo
		info_data[count] = old_data[bidx];
		node->tuples[count] = id;
	};
	auto pick_new = [&](index_t id, index_t aidx, index_t count) {
		// new_id comes before the old id
		// insert the base table data into the update info
		info_data[count] = base_data[id];
		node->nullmask[id] = base_nullmask[id];

		// and insert the update info into the base table
		base_nullmask[id] = update.nullmask[aidx];
		base_data[id] = update_data[aidx];

		node->tuples[count] = id;
	};
	auto pick_old = [&](index_t id, index_t bidx, index_t count) {
		// old_id comes before new_id, insert the old data
		info_data[count] = old_data[bidx];
		node->tuples[count] = id;
	};
	// perform the merge
	node->N = merge_loop(ids, old_ids, update.size(), node->N, vector_offset, merge, pick_new, pick_old);
}

static NumericSegment::merge_update_function_t GetMergeUpdateFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return merge_update_loop<int8_t>;
	case TypeId::INT16:
		return merge_update_loop<int16_t>;
	case TypeId::INT32:
		return merge_update_loop<int32_t>;
	case TypeId::INT64:
		return merge_update_loop<int64_t>;
	case TypeId::FLOAT:
		return merge_update_loop<float>;
	case TypeId::DOUBLE:
		return merge_update_loop<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}
//===--------------------------------------------------------------------===//
// Update Fetch
//===--------------------------------------------------------------------===//
template <class T> static void update_info_fetch(Transaction &transaction, UpdateInfo *info, Vector &result) {
	auto result_data = (T *)result.GetData();
	UpdateInfo::UpdatesForTransaction(info, transaction, [&](UpdateInfo *current) {
		auto info_data = (T *)current->tuple_data;
		for (index_t i = 0; i < current->N; i++) {
			result_data[current->tuples[i]] = info_data[i];
			result.nullmask[current->tuples[i]] = current->nullmask[current->tuples[i]];
		}
	});
}

static NumericSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return update_info_fetch<int8_t>;
	case TypeId::INT16:
		return update_info_fetch<int16_t>;
	case TypeId::INT32:
		return update_info_fetch<int32_t>;
	case TypeId::INT64:
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
// Update Append
//===--------------------------------------------------------------------===//
template <class T>
static void update_info_append(Transaction &transaction, UpdateInfo *info, index_t row_id, Vector &result) {
	auto result_data = (T *)result.GetData();
	UpdateInfo::UpdatesForTransaction(info, transaction, [&](UpdateInfo *current) {
		auto info_data = (T *)current->tuple_data;
		// loop over the tuples in this UpdateInfo
		for (index_t i = 0; i < current->N; i++) {
			if (current->tuples[i] == row_id) {
				// found the relevant tuple
				result_data[result.size()] = info_data[i];
				result.nullmask[result.size()] = current->nullmask[current->tuples[i]];
				break;
			} else if (current->tuples[i] > row_id) {
				// tuples are sorted: so if the current tuple is > row_id we will not find it anymore
				break;
			}
		}
	});
}

static NumericSegment::update_info_append_function_t GetUpdateInfoAppendFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return update_info_append<int8_t>;
	case TypeId::INT16:
		return update_info_append<int16_t>;
	case TypeId::INT32:
		return update_info_append<int32_t>;
	case TypeId::INT64:
		return update_info_append<int64_t>;
	case TypeId::FLOAT:
		return update_info_append<float>;
	case TypeId::DOUBLE:
		return update_info_append<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Rollback Update
//===--------------------------------------------------------------------===//
template <class T> static void rollback_update(UpdateInfo *info, data_ptr_t base) {
	auto &nullmask = *((nullmask_t *)base);
	auto info_data = (T *)info->tuple_data;
	auto base_data = (T *)(base + sizeof(nullmask_t));

	for (index_t i = 0; i < info->N; i++) {
		base_data[info->tuples[i]] = info_data[i];
		nullmask[info->tuples[i]] = info->nullmask[info->tuples[i]];
	}
}

static NumericSegment::rollback_update_function_t GetRollbackUpdateFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return rollback_update<int8_t>;
	case TypeId::INT16:
		return rollback_update<int16_t>;
	case TypeId::INT32:
		return rollback_update<int32_t>;
	case TypeId::INT64:
		return rollback_update<int64_t>;
	case TypeId::FLOAT:
		return rollback_update<float>;
	case TypeId::DOUBLE:
		return rollback_update<double>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}
