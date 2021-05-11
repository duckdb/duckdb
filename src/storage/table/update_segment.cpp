#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"

namespace duckdb {

static UpdateSegment::initialize_update_function_t GetInitializeUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_update_function_t GetFetchUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_committed_function_t GetFetchCommittedFunction(PhysicalType type);
static UpdateSegment::fetch_committed_range_function_t GetFetchCommittedRangeFunction(PhysicalType type);

static UpdateSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type);
static UpdateSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type);
static UpdateSegment::statistics_update_function_t GetStatisticsUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_row_function_t GetFetchRowFunction(PhysicalType type);

UpdateSegment::UpdateSegment(RowGroup &row_group, ColumnData &column_data)
    : row_group(row_group), column_data(column_data), stats(column_data.type) {
	auto physical_type = column_data.type.InternalType();

	this->type_size = GetTypeIdSize(physical_type);

	this->initialize_update_function = GetInitializeUpdateFunction(physical_type);
	this->fetch_update_function = GetFetchUpdateFunction(physical_type);
	this->fetch_committed_function = GetFetchCommittedFunction(physical_type);
	this->fetch_committed_range = GetFetchCommittedRangeFunction(physical_type);
	this->fetch_row_function = GetFetchRowFunction(physical_type);
	this->merge_update_function = GetMergeUpdateFunction(physical_type);
	this->rollback_update_function = GetRollbackUpdateFunction(physical_type);
	this->statistics_update_function = GetStatisticsUpdateFunction(physical_type);
}

UpdateSegment::~UpdateSegment() {
}

void UpdateSegment::ClearUpdates() {
	stats.Reset();
	root.reset();
	heap.Destroy();
}

//===--------------------------------------------------------------------===//
// Update Info Helpers
//===--------------------------------------------------------------------===//
Value UpdateInfo::GetValue(idx_t index) {
	auto &type = segment->column_data.type;

	ValidityMask mask(validity);
	if (!mask.RowIsValid(index)) {
		// null
		return Value(type);
	}
	switch (type.id()) {
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(((int32_t *)tuple_data)[index]);
	default:
		throw NotImplementedException("Unimplemented type for UpdateInfo::GetValue");
	}
}

void UpdateInfo::Print() {
	Printer::Print(ToString());
}

string UpdateInfo::ToString() {
	auto &type = segment->column_data.type;
	string result = "Update Info [" + type.ToString() + ", Count: " + to_string(N) +
	                ", Transaction Id: " + to_string(version_number) + "]\n";
	for (idx_t i = 0; i < N; i++) {
		result += to_string(tuples[i]) + ": " + GetValue(i).ToString() + "\n";
	}
	if (next) {
		result += "\nChild Segment: " + next->ToString();
	}
	return result;
}

void UpdateInfo::Verify() {
#ifdef DEBUG
	for (idx_t i = 1; i < N; i++) {
		D_ASSERT(tuples[i] > tuples[i - 1] && tuples[i] < STANDARD_VECTOR_SIZE);
	}
#endif
}

//===--------------------------------------------------------------------===//
// Update Fetch
//===--------------------------------------------------------------------===//
template <class T>
static void MergeUpdateInfo(UpdateInfo *current, T *result_data, ValidityMask &result_mask) {
	ValidityMask current_mask(current->validity);
	auto info_data = (T *)current->tuple_data;
	if (current->N == STANDARD_VECTOR_SIZE) {
		// special case: update touches ALL tuples of this vector
		// in this case we can just memcpy the data
		// since the layout of the update info is guaranteed to be [0, 1, 2, 3, ...]
		memcpy(result_data, info_data, sizeof(T) * current->N);
		result_mask.EnsureWritable();
		memcpy(result_mask.GetData(), current_mask.GetData(), ValidityMask::STANDARD_MASK_SIZE);
	} else {
		for (idx_t i = 0; i < current->N; i++) {
			result_data[current->tuples[i]] = info_data[i];
			result_mask.Set(current->tuples[i], current_mask.RowIsValidUnsafe(i));
		}
	}
}

template <class T>
static void UpdateMergeFetch(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id, [&](UpdateInfo *current) {
		MergeUpdateInfo<T>(current, result_data, result_mask);
	});
}

static UpdateSegment::fetch_update_function_t GetFetchUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return UpdateMergeFetch<int8_t>;
	case PhysicalType::INT16:
		return UpdateMergeFetch<int16_t>;
	case PhysicalType::INT32:
		return UpdateMergeFetch<int32_t>;
	case PhysicalType::INT64:
		return UpdateMergeFetch<int64_t>;
	case PhysicalType::UINT8:
		return UpdateMergeFetch<uint8_t>;
	case PhysicalType::UINT16:
		return UpdateMergeFetch<uint16_t>;
	case PhysicalType::UINT32:
		return UpdateMergeFetch<uint32_t>;
	case PhysicalType::UINT64:
		return UpdateMergeFetch<uint64_t>;
	case PhysicalType::INT128:
		return UpdateMergeFetch<hugeint_t>;
	case PhysicalType::FLOAT:
		return UpdateMergeFetch<float>;
	case PhysicalType::DOUBLE:
		return UpdateMergeFetch<double>;
	case PhysicalType::INTERVAL:
		return UpdateMergeFetch<interval_t>;
	case PhysicalType::VARCHAR:
		return UpdateMergeFetch<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchUpdates(Transaction &transaction, idx_t vector_index, Vector &result) {
	if (!root) {
		return;
	}
	auto lock_handle = lock.GetSharedLock();
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	fetch_update_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(),
	                      result);
}

//===--------------------------------------------------------------------===//
// Fetch Committed
//===--------------------------------------------------------------------===//
template <class T>
static void TemplatedFetchCommitted(UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	MergeUpdateInfo<T>(info, result_data, result_mask);
}

static UpdateSegment::fetch_committed_function_t GetFetchCommittedFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchCommitted<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchCommitted<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchCommitted<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchCommitted<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchCommitted<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchCommitted<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchCommitted<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchCommitted<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchCommitted<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchCommitted<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchCommitted<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchCommitted<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchCommitted<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchCommitted(idx_t vector_index, Vector &result) {
	if (!root) {
		return;
	}
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	fetch_committed_function(root->info[vector_index]->info.get(), result);
}

//===--------------------------------------------------------------------===//
// Fetch Range
//===--------------------------------------------------------------------===//
template <class T>
static void MergeUpdateInfoRange(UpdateInfo *current, idx_t start, idx_t end, idx_t result_offset, T *result_data,
                                 ValidityMask &result_mask) {
	ValidityMask current_mask(current->validity);
	auto info_data = (T *)current->tuple_data;
	for (idx_t i = 0; i < current->N; i++) {
		auto tuple_idx = current->tuples[i];
		if (tuple_idx < start) {
			continue;
		} else if (i >= end) {
			break;
		}
		auto result_idx = result_offset + tuple_idx;
		result_data[result_idx] = info_data[i];
		result_mask.Set(result_idx, current_mask.RowIsValidUnsafe(i));
	}
}

template <class T>
static void TemplatedFetchCommittedRange(UpdateInfo *info, idx_t start, idx_t end, idx_t result_offset,
                                         Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	MergeUpdateInfoRange<T>(info, start, end, result_offset, result_data, result_mask);
}

static UpdateSegment::fetch_committed_range_function_t GetFetchCommittedRangeFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchCommittedRange<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchCommittedRange<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchCommittedRange<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchCommittedRange<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchCommittedRange<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchCommittedRange<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchCommittedRange<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchCommittedRange<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchCommittedRange<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchCommittedRange<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchCommittedRange<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchCommittedRange<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchCommittedRange<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchCommittedRange(idx_t start_row, idx_t count, Vector &result) {
	if (!root) {
		return;
	}
	idx_t end_row = start_row + count;
	idx_t start_vector = start_row / STANDARD_VECTOR_SIZE;
	idx_t end_vector = end_row / STANDARD_VECTOR_SIZE;
	idx_t result_offset = 0;
	for (idx_t vector_idx = start_vector; vector_idx <= end_vector; vector_idx++) {
		if (!root->info[vector_idx]) {
			continue;
		}
		idx_t start_in_vector = vector_idx == start_vector ? start_row - start_vector * STANDARD_VECTOR_SIZE : 0;
		idx_t end_in_vector =
		    vector_idx == end_vector ? end_row - end_vector * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		fetch_committed_range(root->info[vector_idx]->info.get(), start_in_vector, end_in_vector, result_offset,
		                      result);
	}
}

//===--------------------------------------------------------------------===//
// Fetch Row
//===--------------------------------------------------------------------===//
template <class T>
static void TemplatedFetchRow(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, idx_t row_idx,
                              Vector &result, idx_t result_idx) {
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id, [&](UpdateInfo *current) {
		ValidityMask current_mask(current->validity);
		auto info_data = (T *)current->tuple_data;
		// FIXME: we could do a binary search in here
		for (idx_t i = 0; i < current->N; i++) {
			if (current->tuples[i] == row_idx) {
				result_data[result_idx] = info_data[i];
				result_mask.Set(result_idx, current_mask.RowIsValidUnsafe(i));
				break;
			} else if (current->tuples[i] > row_idx) {
				break;
			}
		}
	});
}

static UpdateSegment::fetch_row_function_t GetFetchRowFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchRow<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchRow<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchRow<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchRow<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchRow<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchRow<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchRow<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchRow<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchRow<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchRow<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchRow<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchRow<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchRow<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment fetch row");
	}
}

void UpdateSegment::FetchRow(Transaction &transaction, idx_t row_id, Vector &result, idx_t result_idx) {
	if (!root) {
		return;
	}
	idx_t vector_index = (row_id - row_group.start) / STANDARD_VECTOR_SIZE;
	if (!root->info[vector_index]) {
		return;
	}
	idx_t row_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;
	fetch_row_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(),
	                   row_in_vector, result, result_idx);
}

//===--------------------------------------------------------------------===//
// Rollback update
//===--------------------------------------------------------------------===//
template <class T>
static void RollbackUpdate(UpdateInfo *base_info, UpdateInfo *rollback_info) {
	auto base_data = (T *)base_info->tuple_data;
	auto rollback_data = (T *)rollback_info->tuple_data;
	ValidityMask base_mask(base_info->validity);
	ValidityMask rollback_mask(rollback_info->validity);

	idx_t base_offset = 0;
	for (idx_t i = 0; i < rollback_info->N; i++) {
		auto id = rollback_info->tuples[i];
		while (base_info->tuples[base_offset] < id) {
			base_offset++;
			D_ASSERT(base_offset < base_info->N);
		}
		base_data[base_offset] = rollback_data[i];
		base_mask.Set(base_offset, rollback_mask.RowIsValidUnsafe(i));
	}
}

static UpdateSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return RollbackUpdate<int8_t>;
	case PhysicalType::INT16:
		return RollbackUpdate<int16_t>;
	case PhysicalType::INT32:
		return RollbackUpdate<int32_t>;
	case PhysicalType::INT64:
		return RollbackUpdate<int64_t>;
	case PhysicalType::UINT8:
		return RollbackUpdate<uint8_t>;
	case PhysicalType::UINT16:
		return RollbackUpdate<uint16_t>;
	case PhysicalType::UINT32:
		return RollbackUpdate<uint32_t>;
	case PhysicalType::UINT64:
		return RollbackUpdate<uint64_t>;
	case PhysicalType::INT128:
		return RollbackUpdate<hugeint_t>;
	case PhysicalType::FLOAT:
		return RollbackUpdate<float>;
	case PhysicalType::DOUBLE:
		return RollbackUpdate<double>;
	case PhysicalType::INTERVAL:
		return RollbackUpdate<interval_t>;
	case PhysicalType::VARCHAR:
		return RollbackUpdate<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

void UpdateSegment::RollbackUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();

	// move the data from the UpdateInfo back into the base info
	D_ASSERT(root->info[info->vector_index]);
	rollback_update_function(root->info[info->vector_index]->info.get(), info);

	// clean up the update chain
	CleanupUpdateInternal(*lock_handle, info);
}

//===--------------------------------------------------------------------===//
// Cleanup Update
//===--------------------------------------------------------------------===//
void UpdateSegment::CleanupUpdateInternal(const StorageLockKey &lock, UpdateInfo *info) {
	D_ASSERT(info->prev);
	auto prev = info->prev;
	prev->next = info->next;
	if (prev->next) {
		prev->next->prev = prev;
	}
}

void UpdateSegment::CleanupUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();
	CleanupUpdateInternal(*lock_handle, info);
}

//===--------------------------------------------------------------------===//
// Check for conflicts in update
//===--------------------------------------------------------------------===//
static void CheckForConflicts(UpdateInfo *info, Transaction &transaction, row_t *ids, idx_t count, row_t offset,
                              UpdateInfo *&node) {
	if (!info) {
		return;
	}
	if (info->version_number == transaction.transaction_id) {
		// this UpdateInfo belongs to the current transaction, set it in the node
		node = info;
	} else if (info->version_number > transaction.start_time) {
		// potential conflict, check that tuple ids do not conflict
		// as both ids and info->tuples are sorted, this is similar to a merge join
		idx_t i = 0, j = 0;
		while (true) {
			auto id = ids[i] - offset;
			if (id == info->tuples[j]) {
				throw TransactionException("Conflict on update!");
			} else if (id < info->tuples[j]) {
				// id < the current tuple in info, move to next id
				i++;
				if (i == count) {
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
	CheckForConflicts(info->next, transaction, ids, count, offset, node);
}

//===--------------------------------------------------------------------===//
// Initialize update info
//===--------------------------------------------------------------------===//
void UpdateSegment::InitializeUpdateInfo(UpdateInfo &info, row_t *ids, idx_t count, idx_t vector_index,
                                         idx_t vector_offset) {
	info.segment = this;
	info.vector_index = vector_index;
	info.prev = nullptr;
	info.next = nullptr;

	// set up the tuple ids
	info.N = count;
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT((idx_t)ids[i] >= vector_offset && (idx_t)ids[i] < vector_offset + STANDARD_VECTOR_SIZE);
		info.tuples[i] = ids[i] - vector_offset;
	};
}

template <class T>
static void InitializeUpdateDataNull(T *__restrict tuple_data, T *__restrict new_data, ValidityMask &tuple_mask,
                                     ValidityMask &new_mask, idx_t count, SegmentStatistics &stats) {
	for (idx_t i = 0; i < count; i++) {
		bool is_valid = tuple_mask.RowIsValid(i);
		tuple_mask.Set(i, is_valid);
		tuple_data[i] = new_data[i];
	}
}

template <class T>
static void InitializeUpdateDataNoNull(T *__restrict tuple_data, T *__restrict new_data, idx_t count,
                                       SegmentStatistics &stats) {
	for (idx_t i = 0; i < count; i++) {
		tuple_data[i] = new_data[i];
	}
}

template <class T>
static void InitializeUpdateData(SegmentStatistics &stats, UpdateInfo *base_info, Vector &base_data,
                                 UpdateInfo *update_info, Vector &update) {
	auto update_data = FlatVector::GetData<T>(update);
	auto &update_mask = FlatVector::Validity(update);
	auto tuple_data = (T *)update_info->tuple_data;

	if (!update_mask.AllValid()) {
		ValidityMask info_mask(update_info->validity);
		for (idx_t i = 0; i < update_info->N; i++) {
			tuple_data[i] = update_data[i];
			info_mask.Set(i, update_mask.RowIsValidUnsafe(i));
		}
	} else {
		for (idx_t i = 0; i < update_info->N; i++) {
			tuple_data[i] = update_data[i];
		}
	}

	auto base_array_data = FlatVector::GetData<T>(base_data);
	auto &base_mask = FlatVector::Validity(base_data);
	auto base_tuple_data = (T *)base_info->tuple_data;
	ValidityMask base_tuple_mask(base_info->validity);
	if (!base_mask.AllValid()) {
		for (idx_t i = 0; i < base_info->N; i++) {
			base_tuple_data[i] = base_array_data[base_info->tuples[i]];
			base_tuple_mask.Set(i, base_mask.RowIsValidUnsafe(base_info->tuples[i]));
		}
	} else {
		for (idx_t i = 0; i < base_info->N; i++) {
			base_tuple_data[i] = base_array_data[base_info->tuples[i]];
		}
	}
}

static UpdateSegment::initialize_update_function_t GetInitializeUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return InitializeUpdateData<int8_t>;
	case PhysicalType::INT16:
		return InitializeUpdateData<int16_t>;
	case PhysicalType::INT32:
		return InitializeUpdateData<int32_t>;
	case PhysicalType::INT64:
		return InitializeUpdateData<int64_t>;
	case PhysicalType::UINT8:
		return InitializeUpdateData<uint8_t>;
	case PhysicalType::UINT16:
		return InitializeUpdateData<uint16_t>;
	case PhysicalType::UINT32:
		return InitializeUpdateData<uint32_t>;
	case PhysicalType::UINT64:
		return InitializeUpdateData<uint64_t>;
	case PhysicalType::INT128:
		return InitializeUpdateData<hugeint_t>;
	case PhysicalType::FLOAT:
		return InitializeUpdateData<float>;
	case PhysicalType::DOUBLE:
		return InitializeUpdateData<double>;
	case PhysicalType::INTERVAL:
		return InitializeUpdateData<interval_t>;
	case PhysicalType::VARCHAR:
		return InitializeUpdateData<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

//===--------------------------------------------------------------------===//
// Merge update info
//===--------------------------------------------------------------------===//
template <class F1, class F2, class F3>
static idx_t MergeLoop(row_t a[], sel_t b[], idx_t acount, idx_t bcount, idx_t aoffset, F1 merge, F2 pick_a,
                       F3 pick_b) {
	idx_t aidx = 0, bidx = 0;
	idx_t count = 0;
	while (aidx < acount && bidx < bcount) {
		auto a_id = a[aidx] - aoffset;
		auto b_id = b[bidx];
		if (a_id == b_id) {
			merge(a_id, aidx, bidx, count);
			aidx++;
			bidx++;
			count++;
		} else if (a_id < b_id) {
			pick_a(a_id, aidx, count);
			aidx++;
			count++;
		} else {
			pick_b(b_id, bidx, count);
			bidx++;
			count++;
		}
	}
	for (; aidx < acount; aidx++) {
		pick_a(a[aidx] - aoffset, aidx, count);
		count++;
	}
	for (; bidx < bcount; bidx++) {
		pick_b(b[bidx], bidx, count);
		count++;
	}
	return count;
}

template <class T>
static void MergeUpdateLoop(SegmentStatistics &stats, UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info,
                            Vector &update, row_t *ids, idx_t count) {
	auto base_id = base_info->segment->row_group.start + base_info->vector_index * STANDARD_VECTOR_SIZE;
#ifdef DEBUG
	// all of these should be sorted, otherwise the below algorithm does not work
	for (idx_t i = 1; i < count; i++) {
		D_ASSERT(ids[i] > ids[i - 1] && ids[i] >= row_t(base_id) && ids[i] < row_t(base_id + STANDARD_VECTOR_SIZE));
	}
#endif

	// we have a new batch of updates (update, ids, count)
	// we already have existing updates (base_info)
	// and potentially, this transaction already has updates present (update_info)
	// we need to merge these all together so that the latest updates get merged into base_info
	// and the "old" values (fetched from EITHER base_info OR from base_data) get placed into update_info
	auto base_table_data = FlatVector::GetData<T>(base_data);
	auto update_vector_data = FlatVector::GetData<T>(update);
	auto base_info_data = (T *)base_info->tuple_data;
	auto update_info_data = (T *)update_info->tuple_data;

	auto &base_table_mask = FlatVector::Validity(base_data);
	auto &update_vector_mask = FlatVector::Validity(update);
	ValidityMask base_info_mask(base_info->validity);
	ValidityMask update_info_mask(update_info->validity);

	// we first do the merging of the old values
	// what we are trying to do here is update the "update_info" of this transaction with all the old data we require
	// this means we need to merge (1) any previously updated values (stored in update_info->tuples)
	// together with (2)
	// to simplify this, we create new arrays here
	// we memcpy these over afterwards
	T result_values[STANDARD_VECTOR_SIZE];
	validity_t result_validity[ValidityMask::STANDARD_ENTRY_COUNT];
	ValidityMask result_mask(result_validity);
	sel_t result_ids[STANDARD_VECTOR_SIZE];

	idx_t base_info_offset = 0;
	idx_t update_info_offset = 0;
	idx_t result_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		// we have to merge the info for "ids[i]"
		auto update_id = ids[i] - base_id;

		while (update_info_offset < update_info->N && update_info->tuples[update_info_offset] < update_id) {
			// old id comes before the current id: write it
			result_values[result_offset] = update_info_data[update_info_offset];
			result_mask.Set(result_offset, update_info_mask.RowIsValidUnsafe(update_info_offset));
			result_ids[result_offset++] = update_info->tuples[update_info_offset];
			update_info_offset++;
		}
		// write the new id
		if (update_info_offset < update_info->N && update_info->tuples[update_info_offset] == update_id) {
			// we have an id that is equivalent in the current update info: write the update info
			result_values[result_offset] = update_info_data[update_info_offset];
			result_mask.Set(result_offset, update_info_mask.RowIsValidUnsafe(update_info_offset));
			result_ids[result_offset++] = update_info->tuples[update_info_offset];
			update_info_offset++;
			continue;
		}

		/// now check if we have the current update_id in the base_info, or if we should fetch it from the base data
		while (base_info_offset < base_info->N && base_info->tuples[base_info_offset] < update_id) {
			base_info_offset++;
		}
		if (base_info_offset < base_info->N && base_info->tuples[base_info_offset] == update_id) {
			// it is! we have to move the tuple from base_info->ids[base_info_offset] to update_info
			result_values[result_offset] = base_info_data[base_info_offset];
			result_mask.Set(result_offset, base_info_mask.RowIsValidUnsafe(base_info_offset));
		} else {
			// it is not! we have to move base_table_data[update_id] to update_info
			result_values[result_offset] = base_table_data[update_id];
			result_mask.Set(result_offset, base_table_mask.RowIsValid(update_id));
		}
		result_ids[result_offset++] = update_id;
	}
	// write any remaining entries from the old updates
	while (update_info_offset < update_info->N) {
		result_values[result_offset] = update_info_data[update_info_offset];
		result_mask.Set(result_offset, update_info_mask.RowIsValidUnsafe(update_info_offset));
		result_ids[result_offset++] = update_info->tuples[update_info_offset];
		update_info_offset++;
	}
	// now copy them back
	update_info->N = result_offset;
	memcpy(update_info_data, result_values, result_offset * sizeof(T));
	memcpy(update_info->validity, result_validity, ValidityMask::STANDARD_ENTRY_COUNT * sizeof(validity_t));
	memcpy(update_info->tuples, result_ids, result_offset * sizeof(sel_t));

	// now we merge the new values into the base_info
	result_offset = 0;
	auto pick_new = [&](idx_t id, idx_t aidx, idx_t count) {
		result_values[result_offset] = update_vector_data[aidx];
		result_mask.Set(result_offset, update_vector_mask.RowIsValid(aidx));
		result_ids[result_offset] = id;
		result_offset++;
	};
	auto pick_old = [&](idx_t id, idx_t bidx, idx_t count) {
		result_values[result_offset] = base_info_data[bidx];
		result_mask.Set(result_offset, base_info_mask.RowIsValidUnsafe(bidx));
		result_ids[result_offset] = id;
		result_offset++;
	};
	// now we perform a merge of the new ids with the old ids
	auto merge = [&](idx_t id, idx_t aidx, idx_t bidx, idx_t count) {
		pick_new(id, aidx, count);
	};
	MergeLoop(ids, base_info->tuples, count, base_info->N, base_id, merge, pick_new, pick_old);

	base_info->N = result_offset;
	memcpy(base_info_data, result_values, result_offset * sizeof(T));
	memcpy(base_info->validity, result_validity, ValidityMask::STANDARD_ENTRY_COUNT * sizeof(validity_t));
	memcpy(base_info->tuples, result_ids, result_offset * sizeof(sel_t));
}

static UpdateSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return MergeUpdateLoop<int8_t>;
	case PhysicalType::INT16:
		return MergeUpdateLoop<int16_t>;
	case PhysicalType::INT32:
		return MergeUpdateLoop<int32_t>;
	case PhysicalType::INT64:
		return MergeUpdateLoop<int64_t>;
	case PhysicalType::UINT8:
		return MergeUpdateLoop<uint8_t>;
	case PhysicalType::UINT16:
		return MergeUpdateLoop<uint16_t>;
	case PhysicalType::UINT32:
		return MergeUpdateLoop<uint32_t>;
	case PhysicalType::UINT64:
		return MergeUpdateLoop<uint64_t>;
	case PhysicalType::INT128:
		return MergeUpdateLoop<hugeint_t>;
	case PhysicalType::FLOAT:
		return MergeUpdateLoop<float>;
	case PhysicalType::DOUBLE:
		return MergeUpdateLoop<double>;
	case PhysicalType::INTERVAL:
		return MergeUpdateLoop<interval_t>;
	case PhysicalType::VARCHAR:
		return MergeUpdateLoop<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update statistics
//===--------------------------------------------------------------------===//
template <class T>
void TemplatedUpdateNumericStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count) {
	auto update_data = FlatVector::GetData<T>(update);
	auto &mask = FlatVector::Validity(update);
	if (mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			NumericStatistics::Update<T>(stats, update_data[i]);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			if (mask.RowIsValid(i)) {
				NumericStatistics::Update<T>(stats, update_data[i]);
			} else {
				auto &validity_stats = (ValidityStatistics &)*stats.statistics->validity_stats;
				validity_stats.has_null = true;
			}
		}
	}
}

void UpdateStringStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count) {
	auto update_data = FlatVector::GetData<string_t>(update);
	auto &mask = FlatVector::Validity(update);
	if (mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			((StringStatistics &)*stats.statistics).Update(update_data[i]);
			if (!update_data[i].IsInlined()) {
				update_data[i] = segment->GetStringHeap().AddString(update_data[i]);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			if (mask.RowIsValid(i)) {
				((StringStatistics &)*stats.statistics).Update(update_data[i]);
				if (!update_data[i].IsInlined()) {
					update_data[i] = segment->GetStringHeap().AddString(update_data[i]);
				}
			} else {
				auto &validity_stats = (ValidityStatistics &)*stats.statistics->validity_stats;
				validity_stats.has_null = true;
			}
		}
	}
}

UpdateSegment::statistics_update_function_t GetStatisticsUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedUpdateNumericStatistics<int8_t>;
	case PhysicalType::INT16:
		return TemplatedUpdateNumericStatistics<int16_t>;
	case PhysicalType::INT32:
		return TemplatedUpdateNumericStatistics<int32_t>;
	case PhysicalType::INT64:
		return TemplatedUpdateNumericStatistics<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedUpdateNumericStatistics<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedUpdateNumericStatistics<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedUpdateNumericStatistics<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedUpdateNumericStatistics<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedUpdateNumericStatistics<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedUpdateNumericStatistics<float>;
	case PhysicalType::DOUBLE:
		return TemplatedUpdateNumericStatistics<double>;
	case PhysicalType::INTERVAL:
		return TemplatedUpdateNumericStatistics<interval_t>;
	case PhysicalType::VARCHAR:
		return UpdateStringStatistics;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void UpdateSegment::Update(Transaction &transaction, Vector &update, row_t *ids, idx_t count, Vector &base_data) {
	// obtain an exclusive lock
	auto write_lock = lock.GetExclusiveLock();

	// update statistics
	statistics_update_function(this, stats, update, count);

#ifdef DEBUG
	// verify that the ids are sorted and there are no duplicates
	for (idx_t i = 1; i < count; i++) {
		D_ASSERT(ids[i] > ids[i - 1]);
	}
#endif

	// create the versions for this segment, if there are none yet
	if (!root) {
		root = make_unique<UpdateNode>();
	}

	// get the vector index based on the first id
	// we assert that all updates must be part of the same vector
	auto first_id = ids[0];
	idx_t vector_index = (first_id - row_group.start) / STANDARD_VECTOR_SIZE;
	idx_t vector_offset = row_group.start + vector_index * STANDARD_VECTOR_SIZE;

	D_ASSERT(idx_t(first_id) >= row_group.start);
	D_ASSERT(vector_index < RowGroup::ROW_GROUP_VECTOR_COUNT);

	// first check the version chain
	UpdateInfo *node = nullptr;

	if (root->info[vector_index]) {
		// there is already a version here, check if there are any conflicts and search for the node that belongs to
		// this transaction in the version chain
		auto base_info = root->info[vector_index]->info.get();
		CheckForConflicts(base_info->next, transaction, ids, count, vector_offset, node);

		// there are no conflicts
		// first, check if this thread has already done any updates
		auto node = base_info->next;
		while (node) {
			if (node->version_number == transaction.transaction_id) {
				// it has! use this node
				break;
			}
			node = node->next;
		}
		if (!node) {
			// no updates made yet by this transaction: initially the update info to empty
			node = transaction.CreateUpdateInfo(type_size, count);
			node->segment = this;
			node->vector_index = vector_index;
			node->N = 0;

			// insert the new node into the chain
			node->next = base_info->next;
			if (node->next) {
				node->next->prev = node;
			}
			node->prev = base_info;
			base_info->next = node;
		}
		base_info->Verify();
		node->Verify();

		// now we are going to perform the merge
		merge_update_function(stats, base_info, base_data, node, update, ids, count);

		base_info->Verify();
		node->Verify();
	} else {
		// there is no version info yet: create the top level update info and fill it with the updates
		auto result = make_unique<UpdateNodeData>();

		result->info = make_unique<UpdateInfo>();
		result->tuples = unique_ptr<sel_t[]>(new sel_t[STANDARD_VECTOR_SIZE]);
		result->tuple_data = unique_ptr<data_t[]>(new data_t[STANDARD_VECTOR_SIZE * type_size]);
		result->info->tuples = result->tuples.get();
		result->info->tuple_data = result->tuple_data.get();
		result->info->version_number = TRANSACTION_ID_START - 1;
		ValidityMask result_mask(result->info->validity);
		result_mask.SetAllValid(STANDARD_VECTOR_SIZE);
		InitializeUpdateInfo(*result->info, ids, count, vector_index, vector_offset);

		// now create the transaction level update info in the undo log
		auto transaction_node = transaction.CreateUpdateInfo(type_size, count);
		InitializeUpdateInfo(*transaction_node, ids, count, vector_index, vector_offset);

		// we write the updates in the
		initialize_update_function(stats, transaction_node, base_data, result->info.get(), update);

		result->info->next = transaction_node;
		result->info->prev = nullptr;
		transaction_node->next = nullptr;
		transaction_node->prev = result->info.get();

		transaction_node->Verify();
		result->info->Verify();

		root->info[vector_index] = move(result);
	}
}

bool UpdateSegment::HasUpdates() const {
	return root.get() != nullptr;
}

bool UpdateSegment::HasUpdates(idx_t vector_index) const {
	if (!HasUpdates()) {
		return false;
	}
	return root->info[vector_index].get();
}

bool UpdateSegment::HasUncommittedUpdates(idx_t vector_index) {
	if (!HasUpdates(vector_index)) {
		return false;
	}
	auto read_lock = lock.GetSharedLock();
	auto entry = root->info[vector_index].get();
	if (entry->info->next) {
		return true;
	}
	return false;
}

bool UpdateSegment::HasUpdates(idx_t start_row_index, idx_t end_row_index) {
	if (!HasUpdates()) {
		return false;
	}
	auto read_lock = lock.GetSharedLock();
	idx_t base_vector_index = start_row_index / STANDARD_VECTOR_SIZE;
	idx_t end_vector_index = end_row_index / STANDARD_VECTOR_SIZE;
	for (idx_t i = base_vector_index; i <= end_vector_index; i++) {
		if (root->info[i]) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
