#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/storage/column_data.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

constexpr const idx_t UpdateSegment::MORSEL_VECTOR_COUNT;
constexpr const idx_t UpdateSegment::MORSEL_SIZE;
constexpr const idx_t UpdateSegment::MORSEL_LAYER_COUNT;
constexpr const idx_t UpdateSegment::MORSEL_LAYER_SIZE;

static UpdateSegment::initialize_update_function_t GetInitializeUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_update_function_t GetFetchUpdateFunction(PhysicalType type);
static UpdateSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type);

UpdateSegment::UpdateSegment(ColumnData &column_data, idx_t start, idx_t count) :
    SegmentBase(start, count), column_data(column_data), stats(column_data.type, GetTypeIdSize(column_data.type.InternalType())) {
	auto physical_type = column_data.type.InternalType();

	this->type_size = GetTypeIdSize(physical_type);

	this->initialize_update_function = GetInitializeUpdateFunction(physical_type);
	this->fetch_update_function = GetFetchUpdateFunction(physical_type);
	this->merge_update_function = GetMergeUpdateFunction(physical_type);
}

UpdateSegment::~UpdateSegment() {

}

//===--------------------------------------------------------------------===//
// Update Fetch
//===--------------------------------------------------------------------===//
template <class T>
static void UpdateMergeFetch(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id, [&](UpdateInfo *current) {
		ValidityMask current_mask(current->validity);
		auto info_data = (T *)current->tuple_data;
		for (idx_t i = 0; i < current->N; i++) {
			result_data[current->tuples[i]] = info_data[i];
			result_mask.Set(current->tuples[i], current_mask.RowIsValidUnsafe(current->tuples[i]));
		}
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
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchUpdates(Transaction &transaction, idx_t vector_index, Vector &result) {
	if (!root) {
		return;
	}
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	fetch_update_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(), result);
}

//===--------------------------------------------------------------------===//
// Rollback update
//===--------------------------------------------------------------------===//
// template <class T>
// static void RollbackUpdate(UpdateSegment *segment, UpdateInfo *info) {
// 	ValidityMask mask(base);
// 	auto info_data = (T *)info->tuple_data;

// 	ValidityMask info_mask(info->validity);
// 	for (idx_t i = 0; i < info->N; i++) {
// 		base_data[info->tuples[i]] = info_data[i];
// 		mask.Set(info->tuples[i], info_mask.RowIsValidUnsafe(info->tuples[i]));
// 	}
// }

// static UpdateSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type) {
// 	switch (type) {
// 	case PhysicalType::BOOL:
// 	case PhysicalType::INT8:
// 		return RollbackUpdate<int8_t>;
// 	case PhysicalType::INT16:
// 		return RollbackUpdate<int16_t>;
// 	case PhysicalType::INT32:
// 		return RollbackUpdate<int32_t>;
// 	case PhysicalType::INT64:
// 		return RollbackUpdate<int64_t>;
// 	case PhysicalType::UINT8:
// 		return RollbackUpdate<uint8_t>;
// 	case PhysicalType::UINT16:
// 		return RollbackUpdate<uint16_t>;
// 	case PhysicalType::UINT32:
// 		return RollbackUpdate<uint32_t>;
// 	case PhysicalType::UINT64:
// 		return RollbackUpdate<uint64_t>;
// 	case PhysicalType::INT128:
// 		return RollbackUpdate<hugeint_t>;
// 	case PhysicalType::FLOAT:
// 		return RollbackUpdate<float>;
// 	case PhysicalType::DOUBLE:
// 		return RollbackUpdate<double>;
// 	case PhysicalType::INTERVAL:
// 		return RollbackUpdate<interval_t>;
// 	default:
// 		throw NotImplementedException("Unimplemented type for uncompressed segment");
// 	}
// }

void UpdateSegment::RollbackUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();

	// move the data from the UpdateInfo back into the base table
	// rollback_update(info);
	CleanupUpdateInternal(*lock_handle, info);

	// FIXME: this doesn't really work with merged updates
	throw NotImplementedException("FIXME: actually roll back values by writing them from update_info to base_info");
	root->info[info->vector_index].reset();

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
void UpdateSegment::InitializeUpdateInfo(UpdateInfo &info, row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset) {
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
static void InitializeUpdateDataNull(T *__restrict tuple_data, T *__restrict new_data,
                           ValidityMask &tuple_mask, ValidityMask &new_mask, idx_t count, SegmentStatistics &stats) {
	for (idx_t i = 0; i < count; i++) {
		bool is_valid = tuple_mask.RowIsValid(i);
		tuple_mask.Set(i, is_valid);
		tuple_data[i] = new_data[i];
		// update the min max with the new data
		if (is_valid) {
			NumericStatistics::Update<T>(stats, new_data[i]);
		} else {
			stats.statistics->has_null = true;
		}
	}
}

template <class T>
static void InitializeUpdateDataNoNull(T *__restrict tuple_data, T *__restrict new_data, idx_t count, SegmentStatistics &stats) {
	for (idx_t i = 0; i < count; i++) {
		tuple_data[i] = new_data[i];
		// update the min max with the new data
		NumericStatistics::Update<T>(stats, new_data[i]);
	}
}

template <class T>
static void InitializeUpdateData(SegmentStatistics &stats, UpdateInfo *info, Vector &update) {
	auto update_data = FlatVector::GetData<T>(update);
	auto &update_mask = FlatVector::Validity(update);
	auto tuple_data = (T *)info->tuple_data;

	if (!update_mask.AllValid()) {
		ValidityMask info_mask(info->validity);
		InitializeUpdateDataNull(tuple_data, update_data, info_mask, update_mask, info->N, stats);
	} else {
		InitializeUpdateDataNoNull(tuple_data, update_data, info->N, stats);
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
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

//===--------------------------------------------------------------------===//
// Merge update info
//===--------------------------------------------------------------------===//
template <class F1, class F2, class F3>
static idx_t merge_loop(row_t a[], sel_t b[], idx_t acount, idx_t bcount, idx_t aoffset, F1 merge, F2 pick_a,
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
static void MergeUpdateLoop(SegmentStatistics &stats, UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update, row_t *ids, idx_t count) {
#ifdef DEBUG
	// all of these should be sorted, otherwise the below algorithm does not work
	for(idx_t i = 1; i < count; i++) {
		D_ASSERT(ids[i] > ids[i - 1] && ids[i] < STANDARD_VECTOR_SIZE);
	}
	for(idx_t i = 1; i < base_info->N; i++) {
		D_ASSERT(base_info->tuples[i] > base_info->tuples[i - 1] && base_info->tuples[i] < STANDARD_VECTOR_SIZE);
	}
	for(idx_t i = 1; i < update_info->N; i++) {
		D_ASSERT(update_info->tuples[i] > update_info->tuples[i - 1] && update_info->tuples[i] < STANDARD_VECTOR_SIZE);
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

	// first update some statistics
	for (idx_t i = 0; i < count; i++) {
		NumericStatistics::Update<T>(stats, update_vector_data[i]);
	}

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
	for(idx_t i = 0; i < count; i++) {
		// we have to merge the info for "ids[i]"
		auto update_id = ids[i];

		while(update_info_offset < update_info->N && update_info->tuples[update_info_offset] < update_id) {
			// old id comes before the current id: write it
			result_values[result_offset] = update_info_data[update_info_offset];
			result_mask.Set(result_offset, update_info_mask.RowIsValidUnsafe(update_info_offset));
			result_ids[result_offset++] = update_info->tuples[update_info_offset];
			update_info_offset++;
		}
		// write the new id
		if (update_info_offset < update_info->N && update_info->tuples[update_info_offset] == update_id) {
			// we have an id that is equivalent in the current update info: overwrite it
			update_info_offset++;
		}

		/// now check if we have the current update_id in the base_info, or if we should fetch it from the base data
		while(base_info_offset < base_info->N && base_info->tuples[base_info_offset] < update_id) {
			base_info_offset++;
		}
		if (base_info_offset < base_info->N && base_info->tuples[base_info_offset] == update_id) {
			// it is! we have to move the tuple from base_info->ids[base_info_offset] to update_info
			auto id = base_info->tuples[base_info_offset];
			result_values[result_offset] = base_info_data[id];
			result_mask.Set(result_offset, base_info_mask.RowIsValidUnsafe(id));
		} else {
			// it is not! we have to move base_table_data[update_id] to update_info
			result_values[result_offset] = base_table_data[update_id];
			result_mask.Set(result_offset, base_table_mask.RowIsValidUnsafe(update_id));
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
	memcpy(update_info_data, result_values, result_offset * sizeof(T));
	memcpy(update_info->validity, result_validity, result_offset * sizeof(sel_t));
	memcpy(update_info->tuples, result_ids, result_offset * sizeof(sel_t));

	// now we merge the new values into the base_info
	result_offset = 0;
	auto pick_new = [&](idx_t id, idx_t aidx, idx_t count) {
		result_values[result_offset] = update_vector_data[aidx];
		result_mask.Set(result_offset, update_vector_mask.RowIsValidUnsafe(aidx));
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
	merge_loop(ids, base_info->tuples, count, base_info->N, 0, merge, pick_new, pick_old);

	memcpy(base_info_data, result_values, result_offset * sizeof(T));
	memcpy(base_info->validity, result_validity, result_offset * sizeof(sel_t));
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
	idx_t vector_index = (first_id - this->start) / STANDARD_VECTOR_SIZE;
	idx_t vector_offset = this->start + vector_index * STANDARD_VECTOR_SIZE;

	D_ASSERT(idx_t(first_id) >= this->start);
	D_ASSERT(vector_index < MORSEL_VECTOR_COUNT);

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
		while(node) {
			if (node->version_number == transaction.transaction_id) {
				// it has! use this node
				break;
			}
		}
		if (!node) {
			// no updates made yet by this transaction: initially the update info to empty
			node = transaction.CreateUpdateInfo(type_size, count);
			InitializeUpdateInfo(*node, ids, count, vector_index, vector_offset);

			// insert the new node into the chain
			node->next = base_info->next;
			node->prev = base_info;
			base_info->next = node;
		}

		// now we are going to perform the merge
		merge_update_function(stats, base_info, base_data, node, update, ids, count);
	} else {
		// there is no version info yet: create the top level update info and fill it with the updates
		auto result = make_unique<UpdateNodeData>();

		result->info = make_unique<UpdateInfo>();
		result->tuples = unique_ptr<sel_t[]>(new sel_t[count]);
		result->tuple_data = unique_ptr<data_t[]>(new data_t[count * type_size]);
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
		initialize_update_function(stats, result->info.get(), update);
		initialize_update_function(stats, transaction_node, base_data);

		result->info->next = transaction_node;
		result->info->prev = nullptr;
		transaction_node->next = nullptr;
		transaction_node->prev = result->info.get();

		root->info[vector_index] = move(result);
	}
}

bool UpdateSegment::HasUpdates() {
	return root.get() != nullptr;
}

}
