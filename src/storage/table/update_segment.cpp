#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/storage/column_data.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

constexpr const idx_t UpdateSegment::MORSEL_VECTOR_COUNT;
constexpr const idx_t UpdateSegment::MORSEL_SIZE;
constexpr const idx_t UpdateSegment::MORSEL_LAYER_COUNT;
constexpr const idx_t UpdateSegment::MORSEL_LAYER_SIZE;


static UpdateSegment::initialize_update_function_t GetUpdateFunction(PhysicalType type);
static UpdateSegment::update_merge_function_t GetMergeUpdateFunction(PhysicalType type);

UpdateSegment::UpdateSegment(ColumnData &column_data, idx_t start, idx_t count) :
    SegmentBase(start, count), column_data(column_data), stats(column_data.type, GetTypeIdSize(column_data.type.InternalType())) {
	auto physical_type = column_data.type.InternalType();

	this->type_size = GetTypeIdSize(physical_type);

	this->initialize_update_function = GetUpdateFunction(physical_type);
	this->update_merge_function = GetMergeUpdateFunction(physical_type);
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
		if (!current->tuple_data) {
			return;
		}
		ValidityMask current_mask(current->validity);
		auto info_data = (T *)current->tuple_data;
		for (idx_t i = 0; i < current->N; i++) {
			result_data[current->tuples[i]] = info_data[i];
			result_mask.Set(current->tuples[i], current_mask.RowIsValidUnsafe(current->tuples[i]));
		}
	});
}

static UpdateSegment::update_merge_function_t GetMergeUpdateFunction(PhysicalType type) {
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

void UpdateSegment::MergeUpdates(Transaction &transaction, idx_t vector_index, Vector &result) {
	if (!root) {
		return;
	}
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	update_merge_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(), result);
}

//===--------------------------------------------------------------------===//
// Check for conflicts in update
//===--------------------------------------------------------------------===//
static void CheckForConflicts(UpdateInfo *info, Transaction &transaction, row_t *ids, idx_t count, row_t offset,
                              UpdateInfo *&node) {
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
	if (info->next) {
		CheckForConflicts(info->next, transaction, ids, count, offset, node);
	}
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

static UpdateSegment::initialize_update_function_t GetUpdateFunction(PhysicalType type) {
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

void UpdateSegment::Update(Transaction &transaction, Vector &update, row_t *ids, idx_t count) {
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
		CheckForConflicts(root->info[vector_index]->info.get(), transaction, ids, count, vector_offset, node);

		throw NotImplementedException("FIXME: merge update info");
	} else {
		// there is no version info yet: create the top level update info and fill it with the updates
		auto result = make_unique<UpdateNodeData>();

		result->info = make_unique<UpdateInfo>();
		result->tuples = unique_ptr<sel_t[]>(new sel_t[count]);
		result->tuple_data = unique_ptr<data_t[]>(new data_t[count * type_size]);
		result->info->tuples = result->tuples.get();
		result->info->tuple_data = result->tuple_data.get();
		InitializeUpdateInfo(*result->info, ids, count, vector_index, vector_offset);
		initialize_update_function(stats, result->info.get(), update);

		// now create the transaction level update info in the undo log
		auto transaction_node = transaction.CreateUpdateInfo(type_size, count);
		InitializeUpdateInfo(*transaction_node, ids, count, vector_index, vector_offset);
		transaction_node->next = result->info.get();
		transaction_node->tuple_data = nullptr;

		root->info[vector_index] = move(result);
	}
}

bool UpdateSegment::HasUpdates() {
	return root.get() != nullptr;
}

}
