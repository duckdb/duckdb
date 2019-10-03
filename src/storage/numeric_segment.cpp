#include "storage/numeric_segment.hpp"
#include "storage/buffer_manager.hpp"
#include "common/types/vector.hpp"
#include "storage/table/append_state.hpp"
#include "transaction/update_info.hpp"
#include "transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

static NumericSegment::append_function_t GetAppendFunction(TypeId type);
static NumericSegment::update_function_t GetUpdateFunction(TypeId type);
static NumericSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(TypeId type);
static NumericSegment::rollback_update_function_t GetRollbackUpdateFunction(TypeId type);
static NumericSegment::merge_update_function_t GetMergeUpdateFunction(TypeId type);
static NumericSegment::update_info_append_function_t GetUpdateInfoAppendFunction(TypeId type);

NumericSegment::NumericSegment(BufferManager &manager, TypeId type) :
	UncompressedSegment(manager, type) {
	// transient segment
	// figure out how many vectors we want to store in this block
	this->append_function = GetAppendFunction(type);
	this->update_function = GetUpdateFunction(type);
	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
	this->append_from_update_info = GetUpdateInfoAppendFunction(type);
	this->rollback_update = GetRollbackUpdateFunction(type);
	this->merge_update_function = GetMergeUpdateFunction(type);

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
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void NumericSegment::Scan(Transaction &transaction, TransientScanState &state, index_t vector_index, Vector &result) {
	auto read_lock = lock.GetSharedLock();
	auto handle = manager.PinBuffer(block_id);

	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	auto data = handle->buffer->data.get();

	auto offset = vector_index * vector_size;

	index_t count = GetVectorCount(vector_index);
	// first fetch the data from the base table
	result.nullmask = *((nullmask_t*) (data + offset));
	memcpy(result.data, data + offset + sizeof(nullmask_t), count * type_size);
	if (versions && versions[vector_index]) {
		// if there are any versions, check if we need to overwrite the data with the versioned data
		fetch_from_update_info(transaction, versions[vector_index], result, count);
	}
	result.count = count;
}

void NumericSegment::Fetch(index_t vector_index, Vector &result) {
	auto read_lock = lock.GetSharedLock();
	auto handle = manager.PinBuffer(block_id);
	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);
	assert(!versions);

	auto data = handle->buffer->data.get();

	auto offset = vector_index * vector_size;

	index_t count = std::min((index_t) STANDARD_VECTOR_SIZE, tuple_count - vector_index * STANDARD_VECTOR_SIZE);

	// no version information: simply set up the data pointer and read the nullmask
	result.nullmask = *((nullmask_t*) (data + offset));
	memcpy(result.data, data + offset + sizeof(nullmask_t), count * type_size);
	result.count = count;
}

//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//
void NumericSegment::IndexScan(TransientScanState &state, index_t vector_index, Vector &result) {
	if (vector_index == 0) {
		// vector_index = 0, obtain a shared lock on the segment that we keep until the index scan is complete
		state.locks.push_back(lock.GetSharedLock());
	}
	if (versions && versions[vector_index]) {
		throw TransactionException("Cannot create index with outstanding updates");
	}
	auto handle = manager.PinBuffer(block_id);

	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	auto data = handle->buffer->data.get();

	auto offset = vector_index * vector_size;

	index_t count = std::min((index_t) STANDARD_VECTOR_SIZE, tuple_count - vector_index * STANDARD_VECTOR_SIZE);

	// no version information: simply set up the data pointer and read the nullmask
	result.nullmask = *((nullmask_t*) (data + offset));
	memcpy(result.data, data + offset + sizeof(nullmask_t), count * type_size);
	result.count = count;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void NumericSegment::Fetch(Transaction &transaction, row_t row_id, Vector &result) {
	auto read_lock = lock.GetSharedLock();
	auto handle = manager.PinBuffer(block_id);

	// get the vector index
	index_t vector_index = row_id / STANDARD_VECTOR_SIZE;
	index_t id_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;

	assert(vector_index < max_vector_count);

	// first fetch the data from the base table
	auto data = handle->buffer->data.get() + vector_index * vector_size;
	auto &nullmask = *((nullmask_t*) (data));
	auto vector_ptr = data + sizeof(nullmask_t);

	result.nullmask[result.count] = nullmask[id_in_vector];
	memcpy(result.data + result.count * type_size, vector_ptr + id_in_vector * type_size, type_size);
	if (versions && versions[vector_index]) {
		// version information: follow the version chain to find out if we need to load this tuple data from any other version
		append_from_update_info(transaction, versions[vector_index], result, id_in_vector);
	}
	result.count++;
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
index_t NumericSegment::Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count) {
	assert(data.type == type);
	auto handle = manager.PinBuffer(block_id);

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
		append_function(stats, handle->buffer->data.get() + vector_size * vector_index, current_tuple_count, data, offset, append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void NumericSegment::Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) {
	if (!node) {
		auto handle = manager.PinBuffer(block_id);

		// create a new node in the undo buffer for this update
		node = CreateUpdateInfo(transaction, ids, update.count, vector_index, vector_offset, type_size);
		// now move the original data into the UpdateInfo
		update_function(stats, node, handle->buffer->data.get() + vector_index * vector_size, update);
	} else {
		// node already exists for this transaction, we need to merge the new updates with the existing updates
		// first check if the updates entirely overlap
		auto handle = manager.PinBuffer(block_id);

		merge_update_function(stats, node, handle->buffer->data.get() + vector_index * vector_size, update, ids, vector_offset);
	}
}

void NumericSegment::RollbackUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();
	auto handle = manager.PinBuffer(block_id);

	// move the data from the UpdateInfo back into the base table
	rollback_update(info, handle->buffer->data.get() + info->vector_index * vector_size);

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

static NumericSegment::append_function_t GetAppendFunction(TypeId type) {
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

static NumericSegment::update_function_t GetUpdateFunction(TypeId type) {
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
// Merge Update
//===--------------------------------------------------------------------===//
template<class T>
static void merge_update_loop(SegmentStatistics &stats, UpdateInfo *node, data_ptr_t base, Vector &update, row_t *ids, index_t vector_offset) {
	auto &base_nullmask = *((nullmask_t *) base);
	auto base_data = (T*) (base + sizeof(nullmask_t));
	auto info_data = (T*) node->tuple_data;
	auto update_data = (T*) update.data;

	// first we copy the old update info into a temporary structure
	sel_t old_ids[STANDARD_VECTOR_SIZE];
	T old_data[STANDARD_VECTOR_SIZE];

	memcpy(old_ids, node->tuples, node->N * sizeof(sel_t));
	memcpy(old_data, node->tuple_data, node->N * sizeof(T));

	// now we perform a merge of the new ids with the old ids
	index_t new_idx = 0, old_idx = 0;
	index_t new_count = 0;
	while(new_idx < update.count && old_idx < node->N) {
		auto new_id = ids[new_idx] - vector_offset;
		if (new_id == old_ids[old_idx]) {
			// new_id and old_id are the same:
			// insert the new data into the base table
			base_nullmask[new_id] = update.nullmask[new_idx];
			base_data[new_id] = update_data[new_idx];
			// insert the old data in the UpdateInfo
			info_data[new_count] = old_data[old_idx];
			node->tuples[new_count] = new_id;

			new_count++;
			new_idx++;
			old_idx++;
		} else if (new_id < old_ids[old_idx]) {
			// new_id comes before the old id
			// insert the base table data into the update info
			info_data[new_count] = base_data[new_id];
			node->nullmask[new_id] = base_nullmask[new_id];

			// and insert the update info into the base table
			base_nullmask[new_id] = update.nullmask[new_idx];
			base_data[new_id] = update_data[new_idx];

			node->tuples[new_count] = new_id;

			new_count++;
			new_idx++;
		} else {
			// old_id comes before new_id, insert the old data
			info_data[new_count] = old_data[old_idx];
			node->tuples[new_count] = old_ids[old_idx];
			new_count++;
			old_idx++;
		}
	}
	// finished merging, insert the remainder of either the old_idx or the new_idx (if any)
	for(; new_idx < update.count; new_idx++, new_count++) {
		auto new_id = ids[new_idx] - vector_offset;
		// insert the base table data into the update info
		info_data[new_count] = base_data[new_id];
		node->nullmask[new_id] = base_nullmask[new_id];

		// and insert the update info into the base table
		base_nullmask[new_id] = update.nullmask[new_idx];
		base_data[new_id] = update_data[new_idx];

		node->tuples[new_count] = new_id;
	}
	for(; old_idx < node->N; old_idx++, new_count++) {
		info_data[new_count] = old_data[old_idx];
		node->tuples[new_count] = old_ids[old_idx];
	}
	node->N = new_count;
}

static NumericSegment::merge_update_function_t GetMergeUpdateFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return merge_update_loop<int8_t>;
	case TypeId::SMALLINT:
		return merge_update_loop<int16_t>;
	case TypeId::INTEGER:
		return merge_update_loop<int32_t>;
	case TypeId::BIGINT:
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
template <class T>
static void update_info_fetch(Transaction &transaction, UpdateInfo *current, Vector &result, index_t count) {
	auto result_data = (T*) result.data;
	while(current) {
		if (current->version_number > transaction.start_time && current->version_number != transaction.transaction_id) {
			// these tuples were either committed AFTER this transaction started or are not committed yet, use tuples stored in this version
			auto info_data = (T*) current->tuple_data;
			for(index_t i = 0; i < current->N; i++) {
				result_data[current->tuples[i]] = info_data[i];
				result.nullmask[current->tuples[i]] = current->nullmask[current->tuples[i]];
			}
		}
		current = current->next;
	}
}

static NumericSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(TypeId type) {
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
// Update Append
//===--------------------------------------------------------------------===//
template <class T>
static void update_info_append(Transaction &transaction, UpdateInfo *current, Vector &result, row_t row_id) {
	auto result_data = (T*) result.data;
	while(current) {
		if (current->version_number > transaction.start_time && current->version_number != transaction.transaction_id) {
			// these tuples were either committed AFTER this transaction started or are not committed yet, use tuples stored in this version
			auto info_data = (T*) current->tuple_data;
			for(index_t i = 0; i < current->N; i++) {
				if (current->tuples[i] == row_id) {
					result_data[result.count] = info_data[i];
					result.nullmask[result.count] = current->nullmask[current->tuples[i]];
					break;
				} else if (current->tuples[i] > row_id) {
					break;
				}
			}
		}
		current = current->next;
	}
}

static NumericSegment::update_info_append_function_t GetUpdateInfoAppendFunction(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return update_info_append<int8_t>;
	case TypeId::SMALLINT:
		return update_info_append<int16_t>;
	case TypeId::INTEGER:
		return update_info_append<int32_t>;
	case TypeId::BIGINT:
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

static NumericSegment::rollback_update_function_t GetRollbackUpdateFunction(TypeId type) {
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
