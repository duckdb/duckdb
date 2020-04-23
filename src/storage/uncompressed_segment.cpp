#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/update_info.hpp"

using namespace duckdb;
using namespace std;

UncompressedSegment::UncompressedSegment(BufferManager &manager, TypeId type, idx_t row_start)
        : manager(manager), type(type), block_id(INVALID_BLOCK), max_vector_count(0), tuple_count(0),
          row_start(row_start),
          versions(nullptr) {
}

UncompressedSegment::~UncompressedSegment() {
    if (block_id >= MAXIMUM_BLOCK) {
        // if the uncompressed segment had an in-memory segment, destroy it when the uncompressed segment is destroyed
        manager.DestroyBuffer(block_id);
    }
}

void UncompressedSegment::Verify(Transaction &transaction) {
#ifdef DEBUG
    ColumnScanState state;
    InitializeScan(state);

    Vector result(this->type);
    for (idx_t i = 0; i < this->tuple_count; i += STANDARD_VECTOR_SIZE) {
        idx_t vector_idx = i / STANDARD_VECTOR_SIZE;
        idx_t count = std::min((idx_t)STANDARD_VECTOR_SIZE, tuple_count - i);
        Scan(transaction, state, vector_idx, result);
        result.Verify(count);
    }
#endif
}

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

void UncompressedSegment::Update(ColumnData &column_data, SegmentStatistics &stats, Transaction &transaction,
                                 Vector &update, row_t *ids, idx_t count, row_t offset) {
    // can only perform in-place updates on temporary blocks
    assert(block_id >= MAXIMUM_BLOCK);

    // obtain an exclusive lock
    auto write_lock = lock.GetExclusiveLock();

#ifdef DEBUG
    // verify that the ids are sorted and there are no duplicates
    for (idx_t i = 1; i < count; i++) {
        assert(ids[i] > ids[i - 1]);
    }
#endif

    // create the versions for this segment, if there are none yet
    if (!versions) {
        this->versions = unique_ptr<UpdateInfo *[]>(new UpdateInfo *[max_vector_count]);
        for (idx_t i = 0; i < max_vector_count; i++) {
            this->versions[i] = nullptr;
        }
    }

    // get the vector index based on the first id
    // we assert that all updates must be part of the same vector
    auto first_id = ids[0];
    idx_t vector_index = (first_id - offset) / STANDARD_VECTOR_SIZE;
    idx_t vector_offset = offset + vector_index * STANDARD_VECTOR_SIZE;

    assert(first_id >= offset);
    assert(vector_index < max_vector_count);

    // first check the version chain
    UpdateInfo *node = nullptr;
    if (versions[vector_index]) {
        // there is already a version here, check if there are any conflicts and search for the node that belongs to
        // this transaction in the version chain
        CheckForConflicts(versions[vector_index], transaction, ids, count, vector_offset, node);
    }
    Update(column_data, stats, transaction, update, ids, count, vector_index, vector_offset, node);
}

UpdateInfo *UncompressedSegment::CreateUpdateInfo(ColumnData &column_data, Transaction &transaction, row_t *ids,
                                                  idx_t count, idx_t vector_index, idx_t vector_offset,
                                                  idx_t type_size) {
    auto node = transaction.CreateUpdateInfo(type_size, STANDARD_VECTOR_SIZE);
    node->column_data = &column_data;
    node->segment = this;
    node->vector_index = vector_index;
    node->prev = nullptr;
    node->next = versions[vector_index];
    if (node->next) {
        node->next->prev = node;
    }
    versions[vector_index] = node;

    // set up the tuple ids
    node->N = count;
    for (idx_t i = 0; i < count; i++) {
        assert((idx_t) ids[i] >= vector_offset && (idx_t) ids[i] < vector_offset + STANDARD_VECTOR_SIZE);
        node->tuples[i] = ids[i] - vector_offset;
    };
    return node;
}

void UncompressedSegment::Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) {
    auto read_lock = lock.GetSharedLock();
    InitializeScan(state);
    FetchBaseData(state, vector_index, result);
}

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
template<class T>
static void filterSelectionType(T *vec, T *predicate, SelectionVector &sel, idx_t &approved_tuple_count,
                                ExpressionType comparison_type, nullmask_t &nullmask) {
    SelectionVector new_sel(approved_tuple_count);
    // the inplace loops take the result as the last parameter
    switch (comparison_type) {
        case ExpressionType::COMPARE_EQUAL: {
            if (nullmask.any()) {
                approved_tuple_count = BinaryExecutor::SelectFlatLoop<T, T, Equals, false, true, true, true, false>(
                        vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            } else {
                approved_tuple_count = BinaryExecutor::SelectFlatLoop<T, T, Equals, false, true, false, true, false>(
                        vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            }
            break;
        }
        case ExpressionType::COMPARE_LESSTHAN: {
            if (nullmask.any()) {
                approved_tuple_count = BinaryExecutor::SelectFlatLoop<T, T, LessThan, false, true, true, true, false>(
                        vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            } else {
                approved_tuple_count = BinaryExecutor::SelectFlatLoop<T, T, LessThan, false, true, false, true, false>(
                        vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            }
            break;
        }
        case ExpressionType::COMPARE_GREATERTHAN: {
            if (nullmask.any()) {
                approved_tuple_count = BinaryExecutor::SelectFlatLoop<T, T, GreaterThan, false, true, true, true, false>(
                        vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            } else {
                approved_tuple_count = BinaryExecutor::SelectFlatLoop<T, T, GreaterThan, false, true, false, true, false>(
                        vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            }
            break;
        }
        case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
            if (nullmask.any()) {
                approved_tuple_count = BinaryExecutor::SelectFlatLoop<T, T, LessThanEquals, false, true, true, true, false>(
                        vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            } else {
                approved_tuple_count =
                        BinaryExecutor::SelectFlatLoop<T, T, LessThanEquals, false, true, false, true, false>(
                                vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            }
            break;
        }
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
            if (nullmask.any()) {
                approved_tuple_count =
                        BinaryExecutor::SelectFlatLoop<T, T, GreaterThanEquals, false, true, true, true, false>(
                                vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            } else {
                approved_tuple_count =
                        BinaryExecutor::SelectFlatLoop<T, T, GreaterThanEquals, false, true, false, true, false>(
                                vec, predicate, &sel, approved_tuple_count, nullmask, &new_sel, &sel);
            }
            break;
        }
        default:
            throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
    }
    sel.Initialize(new_sel);
}

void UncompressedSegment::filterSelection(SelectionVector &sel, Vector &result, TableFilter filter,
                                          idx_t &approved_tuple_count, nullmask_t &nullmask) {
    // the inplace loops take the result as the last parameter
    switch (result.type) {
        case TypeId::INT8: {
            auto result_flat = FlatVector::GetData<int8_t>(result);
            auto predicate_vector = Vector(filter.constant.value_.tinyint);
            auto predicate = FlatVector::GetData<int8_t>(predicate_vector);
            filterSelectionType<int8_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
                                        nullmask);
            break;
        }
        case TypeId::INT16: {
            auto result_flat = FlatVector::GetData<int16_t>(result);
            auto predicate_vector = Vector(filter.constant.value_.smallint);
            auto predicate = FlatVector::GetData<int16_t>(predicate_vector);
            filterSelectionType<int16_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
                                         nullmask);
            break;
        }
        case TypeId::INT32: {
            auto result_flat = FlatVector::GetData<int32_t>(result);
            auto predicate_vector = Vector(filter.constant.value_.integer);
            auto predicate = FlatVector::GetData<int32_t>(predicate_vector);
            filterSelectionType<int32_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
                                         nullmask);
            break;
        }
        case TypeId::INT64: {
            auto result_flat = FlatVector::GetData<int64_t>(result);
            auto predicate_vector = Vector(filter.constant.value_.bigint);
            auto predicate = FlatVector::GetData<int64_t>(predicate_vector);
            filterSelectionType<int64_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
                                         nullmask);
            break;
        }
        case TypeId::FLOAT: {
            auto result_flat = FlatVector::GetData<float>(result);
            auto predicate_vector = Vector(filter.constant.value_.float_);
            auto predicate = FlatVector::GetData<float>(predicate_vector);
            filterSelectionType<float>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
                                       nullmask);
            break;
        }
        case TypeId::DOUBLE: {
            auto result_flat = FlatVector::GetData<double>(result);
            auto predicate_vector = Vector(filter.constant.value_.double_);
            auto predicate = FlatVector::GetData<double>(predicate_vector);
            filterSelectionType<double>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
                                        nullmask);
            break;
        }
        case TypeId::VARCHAR: {
            auto result_flat = FlatVector::GetData<string_t>(result);
            auto predicate_vector = Vector(filter.constant.str_value);
            auto predicate = FlatVector::GetData<string_t>(predicate_vector);
            filterSelectionType<string_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
                                          nullmask);
            break;
        }
        default:
            throw InvalidTypeException(result.type, "Invalid type for filter pushed down to table comparison");
    }
}

void UncompressedSegment::Select(Transaction &transaction, Vector &result, vector<TableFilter> &tableFilters,
                                 SelectionVector &sel, idx_t &approved_tuple_count, ColumnScanState &state) {
    auto read_lock = lock.GetSharedLock();
    if (versions && versions[state.vector_index]) {
        Scan(transaction, state, state.vector_index, result, false);
        auto vector_index = state.vector_index;
        // pin the buffer for this segment
        auto handle = manager.Pin(block_id);
        auto data = handle->node->buffer;
        auto offset = vector_index * vector_size;
        auto source_nullmask = (nullmask_t *) (data + offset);
        for (auto &table_filter : tableFilters) {
            filterSelection(sel, result, table_filter, approved_tuple_count, *source_nullmask);
        }
    } else {
        //! Select the data from the base table
        Select(state, result, sel, approved_tuple_count, tableFilters);
    }
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void UncompressedSegment::Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result,
                               bool get_lock) {
    unique_ptr<StorageLockKey> read_lock;
    if (get_lock) {
        read_lock = lock.GetSharedLock();
    }
    // first fetch the data from the base table
    FetchBaseData(state, vector_index, result);
    if (versions && versions[vector_index]) {
        // if there are any versions, check if we need to overwrite the data with the versioned data
        FetchUpdateData(state, transaction, versions[vector_index], result);
    }
}

void UncompressedSegment::FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result,
                                     SelectionVector &sel, idx_t &approved_tuple_count) {
    auto read_lock = lock.GetSharedLock();
    if (versions && versions[state.vector_index]) {
        // if there are any versions, we do a regular scan
        Scan(transaction, state, state.vector_index, result, false);
        result.Slice(sel, approved_tuple_count);
    } else {
        FilterFetchBaseData(state, result, sel, approved_tuple_count);
    }
}

void UncompressedSegment::IndexScan(ColumnScanState &state, idx_t vector_index, Vector &result) {
    if (vector_index == 0) {
        // vector_index = 0, obtain a shared lock on the segment that we keep until the index scan is complete
        state.locks.push_back(lock.GetSharedLock());
    }
    if (versions && versions[vector_index]) {
        throw TransactionException("Cannot create index with outstanding updates");
    }
    FetchBaseData(state, vector_index, result);
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
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
// ToTemporary
//===--------------------------------------------------------------------===//
void UncompressedSegment::ToTemporary() {
    auto write_lock = lock.GetExclusiveLock();

    if (block_id >= MAXIMUM_BLOCK) {
        // conversion has already been performed by a different thread
        return;
    }
    // pin the current block
    auto current = manager.Pin(block_id);

    // now allocate a new block from the buffer manager
    auto handle = manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
    // now copy the data over and switch to using the new block id
    memcpy(handle->node->buffer, current->node->buffer, Storage::BLOCK_SIZE);
    this->block_id = handle->block_id;
}
