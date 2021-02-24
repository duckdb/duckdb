#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

static NumericSegment::append_function_t GetAppendFunction(PhysicalType type);

static NumericSegment::update_function_t GetUpdateFunction(PhysicalType type);

static NumericSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(PhysicalType type);

static NumericSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type);

static NumericSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type);

static NumericSegment::update_info_append_function_t GetUpdateInfoAppendFunction(PhysicalType type);

NumericSegment::NumericSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start, block_id_t block_id)
    : UncompressedSegment(db, type, row_start) {
	// set up the different functions for this type of segment
	this->append_function = GetAppendFunction(type);
	this->update_function = GetUpdateFunction(type);
	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
	this->append_from_update_info = GetUpdateInfoAppendFunction(type);
	this->rollback_update = GetRollbackUpdateFunction(type);
	this->merge_update_function = GetMergeUpdateFunction(type);

	// figure out how many vectors we want to store in this block
	this->type_size = GetTypeIdSize(type);
	this->vector_size = ValidityMask::STANDARD_MASK_SIZE + type_size * STANDARD_VECTOR_SIZE;
	this->max_vector_count = Storage::BLOCK_SIZE / vector_size;

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id == INVALID_BLOCK) {
		// no block id specified: allocate a buffer for the uncompressed segment
		this->block = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
		auto handle = buffer_manager.Pin(block);
		// initialize masks to 1 for all vectors
		for (idx_t i = 0; i < max_vector_count; i++) {
			ValidityMask mask(handle->node->buffer + (i * vector_size));
			mask.SetAllValid(STANDARD_VECTOR_SIZE);
		}
	} else {
		this->block = buffer_manager.RegisterBlock(block_id);
	}
}

template <class T, class OP>
void Select(SelectionVector &sel, Vector &result, unsigned char *source, ValidityMask &source_mask, T constant,
            idx_t &approved_tuple_count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData(result);
	SelectionVector new_sel(approved_tuple_count);
	idx_t result_count = 0;
	if (!source_mask.AllValid()) {
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			idx_t src_idx = sel.get_index(i);
			bool comparison_result = source_mask.RowIsValid(src_idx) && OP::Operation(((T *)source)[src_idx], constant);
			((T *)result_data)[src_idx] = ((T *)source)[src_idx];
			new_sel.set_index(result_count, src_idx);
			result_count += comparison_result;
		}
	} else {
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			idx_t src_idx = sel.get_index(i);
			bool comparison_result = OP::Operation(((T *)source)[src_idx], constant);
			((T *)result_data)[src_idx] = ((T *)source)[src_idx];
			new_sel.set_index(result_count, src_idx);
			result_count += comparison_result;
		}
	}
	sel.Initialize(new_sel);
	approved_tuple_count = result_count;
}

template <class T, class OPL, class OPR>
void Select(SelectionVector &sel, Vector &result, unsigned char *source, ValidityMask &source_mask,
            const T constant_left, const T constant_right, idx_t &approved_tuple_count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData(result);
	SelectionVector new_sel(approved_tuple_count);
	idx_t result_count = 0;
	if (!source_mask.AllValid()) {
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			idx_t src_idx = sel.get_index(i);
			bool comparison_result = source_mask.RowIsValid(src_idx) &&
			                         OPL::Operation(((T *)source)[src_idx], constant_left) &&
			                         OPR::Operation(((T *)source)[src_idx], constant_right);
			((T *)result_data)[src_idx] = ((T *)source)[src_idx];
			new_sel.set_index(result_count, src_idx);
			result_count += comparison_result;
		}
	} else {
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			idx_t src_idx = sel.get_index(i);
			bool comparison_result = OPL::Operation(((T *)source)[src_idx], constant_left) &&
			                         OPR::Operation(((T *)source)[src_idx], constant_right);
			((T *)result_data)[src_idx] = ((T *)source)[src_idx];
			new_sel.set_index(result_count, src_idx);
			result_count += comparison_result;
		}
	}
	sel.Initialize(new_sel);
	approved_tuple_count = result_count;
}

template <class OP>
static void TemplatedSelectOperation(SelectionVector &sel, Vector &result, PhysicalType type, unsigned char *source,
                                     ValidityMask &source_mask, Value &constant, idx_t &approved_tuple_count) {
	// the inplace loops take the result as the last parameter
	switch (type) {
	case PhysicalType::UINT8: {
		Select<uint8_t, OP>(sel, result, source, source_mask, constant.value_.utinyint, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT16: {
		Select<uint16_t, OP>(sel, result, source, source_mask, constant.value_.usmallint, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT32: {
		Select<uint32_t, OP>(sel, result, source, source_mask, constant.value_.uinteger, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT64: {
		Select<uint64_t, OP>(sel, result, source, source_mask, constant.value_.ubigint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT8: {
		Select<int8_t, OP>(sel, result, source, source_mask, constant.value_.tinyint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT16: {
		Select<int16_t, OP>(sel, result, source, source_mask, constant.value_.smallint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT32: {
		Select<int32_t, OP>(sel, result, source, source_mask, constant.value_.integer, approved_tuple_count);
		break;
	}
	case PhysicalType::INT64: {
		Select<int64_t, OP>(sel, result, source, source_mask, constant.value_.bigint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT128: {
		Select<hugeint_t, OP>(sel, result, source, source_mask, constant.value_.hugeint, approved_tuple_count);
		break;
	}
	case PhysicalType::FLOAT: {
		Select<float, OP>(sel, result, source, source_mask, constant.value_.float_, approved_tuple_count);
		break;
	}
	case PhysicalType::DOUBLE: {
		Select<double, OP>(sel, result, source, source_mask, constant.value_.double_, approved_tuple_count);
		break;
	}
	case PhysicalType::BOOL: {
		Select<bool, OP>(sel, result, source, source_mask, constant.value_.boolean, approved_tuple_count);
		break;
	}
	default:
		throw InvalidTypeException(type, "Invalid type for filter pushed down to table comparison");
	}
}

template <class OPL, class OPR>
static void TemplatedSelectOperationBetween(SelectionVector &sel, Vector &result, PhysicalType type,
                                            unsigned char *source, ValidityMask &source_mask, Value &constant_left,
                                            Value &constant_right, idx_t &approved_tuple_count) {
	// the inplace loops take the result as the last parameter
	switch (type) {
	case PhysicalType::UINT8: {
		Select<uint8_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.utinyint,
		                          constant_right.value_.utinyint, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT16: {
		Select<uint16_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.usmallint,
		                           constant_right.value_.usmallint, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT32: {
		Select<uint32_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.uinteger,
		                           constant_right.value_.uinteger, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT64: {
		Select<uint64_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.ubigint,
		                           constant_right.value_.ubigint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT8: {
		Select<int8_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.tinyint,
		                         constant_right.value_.tinyint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT16: {
		Select<int16_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.smallint,
		                          constant_right.value_.smallint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT32: {
		Select<int32_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.integer,
		                          constant_right.value_.integer, approved_tuple_count);
		break;
	}
	case PhysicalType::INT64: {
		Select<int64_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.bigint,
		                          constant_right.value_.bigint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT128: {
		Select<hugeint_t, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.hugeint,
		                            constant_right.value_.hugeint, approved_tuple_count);
		break;
	}
	case PhysicalType::FLOAT: {
		Select<float, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.float_,
		                        constant_right.value_.float_, approved_tuple_count);
		break;
	}
	case PhysicalType::DOUBLE: {
		Select<double, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.double_,
		                         constant_right.value_.double_, approved_tuple_count);
		break;
	}
	case PhysicalType::BOOL: {
		Select<bool, OPL, OPR>(sel, result, source, source_mask, constant_left.value_.boolean,
		                       constant_right.value_.boolean, approved_tuple_count);
		break;
	}
	default:
		throw InvalidTypeException(type, "Invalid type for filter pushed down to table comparison");
	}
}

void NumericSegment::Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
                            vector<TableFilter> &table_filter) {
	auto vector_index = state.vector_index;
	D_ASSERT(vector_index < max_vector_count);
	D_ASSERT(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	// pin the buffer for this segment
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);
	auto data = handle->node->buffer;
	auto offset = vector_index * vector_size;
	auto source_mask_ptr = (data_ptr_t)(data + offset);
	ValidityMask source_mask(source_mask_ptr);
	auto source_data = data + offset + ValidityMask::STANDARD_MASK_SIZE;

	if (table_filter.size() == 1) {
		switch (table_filter[0].comparison_type) {
		case ExpressionType::COMPARE_EQUAL: {
			TemplatedSelectOperation<Equals>(sel, result, state.current->type.InternalType(), source_data, source_mask,
			                                 table_filter[0].constant, approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_LESSTHAN: {
			TemplatedSelectOperation<LessThan>(sel, result, state.current->type.InternalType(), source_data,
			                                   source_mask, table_filter[0].constant, approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_GREATERTHAN: {
			TemplatedSelectOperation<GreaterThan>(sel, result, state.current->type.InternalType(), source_data,
			                                      source_mask, table_filter[0].constant, approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
			TemplatedSelectOperation<LessThanEquals>(sel, result, state.current->type.InternalType(), source_data,
			                                         source_mask, table_filter[0].constant, approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
			TemplatedSelectOperation<GreaterThanEquals>(sel, result, state.current->type.InternalType(), source_data,
			                                            source_mask, table_filter[0].constant, approved_tuple_count);
			break;
		}
		default:
			throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
		}
	} else {
		D_ASSERT(table_filter[0].comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
		         table_filter[0].comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO);
		D_ASSERT(table_filter[1].comparison_type == ExpressionType::COMPARE_LESSTHAN ||
		         table_filter[1].comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO);

		if (table_filter[0].comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
			if (table_filter[1].comparison_type == ExpressionType::COMPARE_LESSTHAN) {
				TemplatedSelectOperationBetween<GreaterThan, LessThan>(
				    sel, result, state.current->type.InternalType(), source_data, source_mask, table_filter[0].constant,
				    table_filter[1].constant, approved_tuple_count);
			} else {
				TemplatedSelectOperationBetween<GreaterThan, LessThanEquals>(
				    sel, result, state.current->type.InternalType(), source_data, source_mask, table_filter[0].constant,
				    table_filter[1].constant, approved_tuple_count);
			}
		} else {
			if (table_filter[1].comparison_type == ExpressionType::COMPARE_LESSTHAN) {
				TemplatedSelectOperationBetween<GreaterThanEquals, LessThan>(
				    sel, result, state.current->type.InternalType(), source_data, source_mask, table_filter[0].constant,
				    table_filter[1].constant, approved_tuple_count);
			} else {
				TemplatedSelectOperationBetween<GreaterThanEquals, LessThanEquals>(
				    sel, result, state.current->type.InternalType(), source_data, source_mask, table_filter[0].constant,
				    table_filter[1].constant, approved_tuple_count);
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void NumericSegment::InitializeScan(ColumnScanState &state) {
	// pin the primary buffer
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	state.primary_handle = buffer_manager.Pin(block);
}

//===--------------------------------------------------------------------===//
// Fetch base data
//===--------------------------------------------------------------------===//
void NumericSegment::FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) {
	D_ASSERT(vector_index < max_vector_count);
	D_ASSERT(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	auto data = state.primary_handle->node->buffer;
	auto offset = vector_index * vector_size;

	idx_t count = GetVectorCount(vector_index);

	auto &result_mask = FlatVector::Validity(result);
	ValidityMask source_mask(data + offset);
	if (!source_mask.CheckAllValid(count)) {
		result_mask.Copy(source_mask, count);
	}

	auto source_data = data + offset + ValidityMask::STANDARD_MASK_SIZE;

	// fetch the nullmask and copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result), source_data, count * type_size);
}

void NumericSegment::FetchUpdateData(ColumnScanState &state, transaction_t start_time, transaction_t transaction_id,
                                     UpdateInfo *version, Vector &result) {
	fetch_from_update_info(start_time, transaction_id, version, result);
}

template <class T>
static void TemplatedAssignment(SelectionVector &sel, data_ptr_t source, data_ptr_t result, ValidityMask &source_mask,
                                ValidityMask &result_mask, idx_t approved_tuple_count) {
	auto source_data = (T *)source;
	auto result_data = (T *)result;
	if (!source_mask.AllValid()) {
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			if (!source_mask.RowIsValidUnsafe(sel.get_index(i))) {
				result_mask.SetInvalid(i);
			} else {
				result_data[i] = source_data[sel.get_index(i)];
			}
		}
	} else {
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			result_data[i] = source_data[sel.get_index(i)];
		}
	}
}

void NumericSegment::FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
                                         idx_t &approved_tuple_count) {
	auto vector_index = state.vector_index;
	D_ASSERT(vector_index < max_vector_count);
	D_ASSERT(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	// pin the buffer for this segment
	auto data = state.primary_handle->node->buffer;

	auto offset = vector_index * vector_size;
	auto source_mask_ptr = (data_ptr_t)(data + offset);
	auto source_data = data + offset + ValidityMask::STANDARD_MASK_SIZE;

	ValidityMask source_mask(source_mask_ptr);
	// fetch the nullmask and copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData(result);
	auto &result_mask = FlatVector::Validity(result);
	// the inplace loops take the result as the last parameter
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8: {
		TemplatedAssignment<int8_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::INT16: {
		TemplatedAssignment<int16_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::INT32: {
		TemplatedAssignment<int32_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::INT64: {
		TemplatedAssignment<int64_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT8: {
		TemplatedAssignment<uint8_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT16: {
		TemplatedAssignment<uint16_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT32: {
		TemplatedAssignment<uint32_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::UINT64: {
		TemplatedAssignment<uint64_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::INT128: {
		TemplatedAssignment<hugeint_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::FLOAT: {
		TemplatedAssignment<float>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::DOUBLE: {
		TemplatedAssignment<double>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	case PhysicalType::INTERVAL: {
		TemplatedAssignment<interval_t>(sel, source_data, result_data, source_mask, result_mask, approved_tuple_count);
		break;
	}
	default:
		throw InvalidTypeException(type, "Invalid type for filter scan");
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void NumericSegment::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
                              idx_t result_idx) {
	auto read_lock = lock.GetSharedLock();
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	// get the vector index
	idx_t vector_index = row_id / STANDARD_VECTOR_SIZE;
	idx_t id_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;
	D_ASSERT(vector_index < max_vector_count);

	// first fetch the data from the base table
	auto data = handle->node->buffer + vector_index * vector_size;
	auto vector_ptr = data + ValidityMask::STANDARD_MASK_SIZE;

	ValidityMask source_mask(data);

	FlatVector::SetNull(result, result_idx, !source_mask.RowIsValid(id_in_vector));
	memcpy(FlatVector::GetData(result) + result_idx * type_size, vector_ptr + id_in_vector * type_size, type_size);
	if (versions && versions[vector_index]) {
		// version information: follow the version chain to find out if we need to load this tuple data from any other
		// version
		append_from_update_info(transaction, versions[vector_index], id_in_vector, result, result_idx);
	}
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t NumericSegment::Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) {
	D_ASSERT(data.GetType().InternalType() == type);
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	idx_t initial_count = tuple_count;
	while (count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		idx_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			break;
		}
		idx_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		idx_t append_count = MinValue(STANDARD_VECTOR_SIZE - current_tuple_count, count);

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
                            row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (!node) {
		auto handle = buffer_manager.Pin(block);

		// create a new node in the undo buffer for this update
		node = CreateUpdateInfo(column_data, transaction, ids, count, vector_index, vector_offset, type_size);
		// now move the original data into the UpdateInfo
		update_function(stats, node, handle->node->buffer + vector_index * vector_size, update);
	} else {
		// node already exists for this transaction, we need to merge the new updates with the existing updates
		auto handle = buffer_manager.Pin(block);

		merge_update_function(stats, node, handle->node->buffer + vector_index * vector_size, update, ids, count,
		                      vector_offset);
	}
}

void NumericSegment::RollbackUpdate(UpdateInfo *info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	// move the data from the UpdateInfo back into the base table
	rollback_update(info, handle->node->buffer + info->vector_index * vector_size);

	CleanupUpdate(info);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T>
static inline void UpdateNumericStatisticsInternal(T new_value, T &min, T &max) {
	if (LessThan::Operation(new_value, min)) {
		min = new_value;
	}
	if (GreaterThan::Operation(new_value, max)) {
		max = new_value;
	}
}

template <class T>
static inline void UpdateNumericStatistics(SegmentStatistics &stats, T new_value);

template <>
inline void UpdateNumericStatistics<int8_t>(SegmentStatistics &stats, int8_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<int8_t>(new_value, nstats.min.value_.tinyint, nstats.max.value_.tinyint);
}

template <>
inline void UpdateNumericStatistics<int16_t>(SegmentStatistics &stats, int16_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<int16_t>(new_value, nstats.min.value_.smallint, nstats.max.value_.smallint);
}

template <>
inline void UpdateNumericStatistics<int32_t>(SegmentStatistics &stats, int32_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<int32_t>(new_value, nstats.min.value_.integer, nstats.max.value_.integer);
}

template <>
inline void UpdateNumericStatistics<int64_t>(SegmentStatistics &stats, int64_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<int64_t>(new_value, nstats.min.value_.bigint, nstats.max.value_.bigint);
}

template <>
inline void UpdateNumericStatistics<uint8_t>(SegmentStatistics &stats, uint8_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<uint8_t>(new_value, nstats.min.value_.utinyint, nstats.max.value_.utinyint);
}

template <>
inline void UpdateNumericStatistics<uint16_t>(SegmentStatistics &stats, uint16_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<uint16_t>(new_value, nstats.min.value_.usmallint, nstats.max.value_.usmallint);
}

template <>
inline void UpdateNumericStatistics<uint32_t>(SegmentStatistics &stats, uint32_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<uint32_t>(new_value, nstats.min.value_.uinteger, nstats.max.value_.uinteger);
}

template <>
inline void UpdateNumericStatistics<uint64_t>(SegmentStatistics &stats, uint64_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<uint64_t>(new_value, nstats.min.value_.ubigint, nstats.max.value_.ubigint);
}

template <>
inline void UpdateNumericStatistics<hugeint_t>(SegmentStatistics &stats, hugeint_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<hugeint_t>(new_value, nstats.min.value_.hugeint, nstats.max.value_.hugeint);
}

template <>
inline void UpdateNumericStatistics<float>(SegmentStatistics &stats, float new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<float>(new_value, nstats.min.value_.float_, nstats.max.value_.float_);
}

template <>
inline void UpdateNumericStatistics<double>(SegmentStatistics &stats, double new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateNumericStatisticsInternal<double>(new_value, nstats.min.value_.double_, nstats.max.value_.double_);
}

template <>
void UpdateNumericStatistics<interval_t>(SegmentStatistics &stats, interval_t new_value) {
}

template <class T>
static void AppendLoop(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, Vector &source, idx_t offset,
                       idx_t count) {
	ValidityMask mask(target);

	VectorData adata;
	source.Orrify(count, adata);

	auto sdata = (T *)adata.data;
	auto tdata = (T *)(target + ValidityMask::STANDARD_MASK_SIZE);
	if (!adata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = !adata.validity.RowIsValid(source_idx);
			if (is_null) {
				mask.SetInvalidUnsafe(target_idx);
				stats.statistics->has_null = true;
			} else {
				UpdateNumericStatistics<T>(stats, sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			UpdateNumericStatistics<T>(stats, sdata[source_idx]);
			tdata[target_idx] = sdata[source_idx];
		}
	}
}

static NumericSegment::append_function_t GetAppendFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return AppendLoop<int8_t>;
	case PhysicalType::INT16:
		return AppendLoop<int16_t>;
	case PhysicalType::INT32:
		return AppendLoop<int32_t>;
	case PhysicalType::INT64:
		return AppendLoop<int64_t>;
	case PhysicalType::UINT8:
		return AppendLoop<uint8_t>;
	case PhysicalType::UINT16:
		return AppendLoop<uint16_t>;
	case PhysicalType::UINT32:
		return AppendLoop<uint32_t>;
	case PhysicalType::UINT64:
		return AppendLoop<uint64_t>;
	case PhysicalType::INT128:
		return AppendLoop<hugeint_t>;
	case PhysicalType::FLOAT:
		return AppendLoop<float>;
	case PhysicalType::DOUBLE:
		return AppendLoop<double>;
	case PhysicalType::INTERVAL:
		return AppendLoop<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
template <class T>
static void UpdateLoopNull(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data,
                           ValidityMask &undo_mask, ValidityMask &base_mask, ValidityMask &new_mask, idx_t count,
                           sel_t *__restrict base_sel, SegmentStatistics &stats) {
	for (idx_t i = 0; i < count; i++) {
		bool is_valid = new_mask.RowIsValid(i);
		// first move the base data into the undo buffer info
		undo_data[i] = base_data[base_sel[i]];
		undo_mask.Set(base_sel[i], base_mask.RowIsValid(base_sel[i]));
		// now move the new data in-place into the base table
		base_data[base_sel[i]] = new_data[i];
		base_mask.Set(base_sel[i], is_valid);
		// update the min max with the new data
		if (!is_valid) {
			stats.statistics->has_null = true;
		} else {
			UpdateNumericStatistics<T>(stats, new_data[i]);
		}
	}
}

template <class T>
static void UpdateLoopNoNull(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data, idx_t count,
                             sel_t *__restrict base_sel, SegmentStatistics &stats) {
	for (idx_t i = 0; i < count; i++) {
		// first move the base data into the undo buffer info
		undo_data[i] = base_data[base_sel[i]];
		// now move the new data in-place into the base table
		base_data[base_sel[i]] = new_data[i];
		// update the min max with the new data
		UpdateNumericStatistics<T>(stats, new_data[i]);
	}
}

template <class T>
static void UpdateLoop(SegmentStatistics &stats, UpdateInfo *info, data_ptr_t base, Vector &update) {
	auto update_data = FlatVector::GetData<T>(update);
	auto &update_mask = FlatVector::Validity(update);
	ValidityMask base_mask(base);
	auto base_data = (T *)(base + ValidityMask::STANDARD_MASK_SIZE);
	auto undo_data = (T *)info->tuple_data;

	if (!update_mask.AllValid() || !base_mask.AllValid()) {
		ValidityMask info_mask(info->validity);
		UpdateLoopNull(undo_data, base_data, update_data, info_mask, base_mask, update_mask, info->N, info->tuples,
		               stats);
	} else {
		UpdateLoopNoNull(undo_data, base_data, update_data, info->N, info->tuples, stats);
	}
}

static NumericSegment::update_function_t GetUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return UpdateLoop<int8_t>;
	case PhysicalType::INT16:
		return UpdateLoop<int16_t>;
	case PhysicalType::INT32:
		return UpdateLoop<int32_t>;
	case PhysicalType::INT64:
		return UpdateLoop<int64_t>;
	case PhysicalType::UINT8:
		return UpdateLoop<uint8_t>;
	case PhysicalType::UINT16:
		return UpdateLoop<uint16_t>;
	case PhysicalType::UINT32:
		return UpdateLoop<uint32_t>;
	case PhysicalType::UINT64:
		return UpdateLoop<uint64_t>;
	case PhysicalType::INT128:
		return UpdateLoop<hugeint_t>;
	case PhysicalType::FLOAT:
		return UpdateLoop<float>;
	case PhysicalType::DOUBLE:
		return UpdateLoop<double>;
	case PhysicalType::INTERVAL:
		return UpdateLoop<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Merge Update
//===--------------------------------------------------------------------===//
template <class T>
static void MergeUpdateLoop(SegmentStatistics &stats, UpdateInfo *node, data_ptr_t base, Vector &update, row_t *ids,
                            idx_t count, idx_t vector_offset) {
	ValidityMask base_mask(base);
	auto base_data = (T *)(base + ValidityMask::STANDARD_MASK_SIZE);
	auto info_data = (T *)node->tuple_data;
	auto update_data = FlatVector::GetData<T>(update);
	auto &update_mask = FlatVector::Validity(update);
	for (idx_t i = 0; i < count; i++) {
		UpdateNumericStatistics<T>(stats, update_data[i]);
	}

	// first we copy the old update info into a temporary structure
	sel_t old_ids[STANDARD_VECTOR_SIZE];
	T old_data[STANDARD_VECTOR_SIZE];

	memcpy(old_ids, node->tuples, node->N * sizeof(sel_t));
	memcpy(old_data, node->tuple_data, node->N * sizeof(T));

	// now we perform a merge of the new ids with the old ids
	auto merge = [&](idx_t id, idx_t aidx, idx_t bidx, idx_t count) {
		// new_id and old_id are the same:
		// insert the new data into the base table
		base_mask.Set(id, update_mask.RowIsValid(aidx));
		base_data[id] = update_data[aidx];
		// insert the old data in the UpdateInfo
		info_data[count] = old_data[bidx];
		node->tuples[count] = id;
	};

	ValidityMask node_mask(node->validity);
	auto pick_new = [&](idx_t id, idx_t aidx, idx_t count) {
		// new_id comes before the old id
		// insert the base table data into the update info
		info_data[count] = base_data[id];
		node_mask.Set(id, base_mask.RowIsValid(id));

		// and insert the update info into the base table
		base_mask.Set(id, update_mask.RowIsValid(aidx));
		base_data[id] = update_data[aidx];

		node->tuples[count] = id;
	};
	auto pick_old = [&](idx_t id, idx_t bidx, idx_t count) {
		// old_id comes before new_id, insert the old data
		info_data[count] = old_data[bidx];
		node->tuples[count] = id;
	};
	// perform the merge
	node->N = merge_loop(ids, old_ids, count, node->N, vector_offset, merge, pick_new, pick_old);
}

static NumericSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type) {
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
// Update Fetch
//===--------------------------------------------------------------------===//
template <class T>
static void UpdateInfoFetch(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, Vector &result) {
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

static NumericSegment::update_info_fetch_function_t GetUpdateInfoFetchFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return UpdateInfoFetch<int8_t>;
	case PhysicalType::INT16:
		return UpdateInfoFetch<int16_t>;
	case PhysicalType::INT32:
		return UpdateInfoFetch<int32_t>;
	case PhysicalType::INT64:
		return UpdateInfoFetch<int64_t>;
	case PhysicalType::UINT8:
		return UpdateInfoFetch<uint8_t>;
	case PhysicalType::UINT16:
		return UpdateInfoFetch<uint16_t>;
	case PhysicalType::UINT32:
		return UpdateInfoFetch<uint32_t>;
	case PhysicalType::UINT64:
		return UpdateInfoFetch<uint64_t>;
	case PhysicalType::INT128:
		return UpdateInfoFetch<hugeint_t>;
	case PhysicalType::FLOAT:
		return UpdateInfoFetch<float>;
	case PhysicalType::DOUBLE:
		return UpdateInfoFetch<double>;
	case PhysicalType::INTERVAL:
		return UpdateInfoFetch<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update Append
//===--------------------------------------------------------------------===//
template <class T>
static void UpdateInfoAppend(Transaction &transaction, UpdateInfo *info, idx_t row_id, Vector &result,
                             idx_t result_idx) {
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(
	    info, transaction.start_time, transaction.transaction_id, [&](UpdateInfo *current) {
		    auto info_data = (T *)current->tuple_data;
		    ValidityMask current_mask(current->validity);
		    // loop over the tuples in this UpdateInfo
		    for (idx_t i = 0; i < current->N; i++) {
			    if (current->tuples[i] == row_id) {
				    // found the relevant tuple
				    result_data[result_idx] = info_data[i];
				    result_mask.Set(result_idx, current_mask.RowIsValidUnsafe(current->tuples[i]));
				    break;
			    } else if (current->tuples[i] > row_id) {
				    // tuples are sorted: so if the current tuple is > row_id we will not
				    // find it anymore
				    break;
			    }
		    }
	    });
}

static NumericSegment::update_info_append_function_t GetUpdateInfoAppendFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return UpdateInfoAppend<int8_t>;
	case PhysicalType::INT16:
		return UpdateInfoAppend<int16_t>;
	case PhysicalType::INT32:
		return UpdateInfoAppend<int32_t>;
	case PhysicalType::INT64:
		return UpdateInfoAppend<int64_t>;
	case PhysicalType::UINT8:
		return UpdateInfoAppend<uint8_t>;
	case PhysicalType::UINT16:
		return UpdateInfoAppend<uint16_t>;
	case PhysicalType::UINT32:
		return UpdateInfoAppend<uint32_t>;
	case PhysicalType::UINT64:
		return UpdateInfoAppend<uint64_t>;
	case PhysicalType::INT128:
		return UpdateInfoAppend<hugeint_t>;
	case PhysicalType::FLOAT:
		return UpdateInfoAppend<float>;
	case PhysicalType::DOUBLE:
		return UpdateInfoAppend<double>;
	case PhysicalType::INTERVAL:
		return UpdateInfoAppend<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Rollback Update
//===--------------------------------------------------------------------===//
template <class T>
static void RollbackUpdate(UpdateInfo *info, data_ptr_t base) {
	ValidityMask mask(base);
	auto info_data = (T *)info->tuple_data;
	auto base_data = (T *)(base + ValidityMask::STANDARD_MASK_SIZE);

	ValidityMask info_mask(info->validity);
	for (idx_t i = 0; i < info->N; i++) {
		base_data[info->tuples[i]] = info_data[i];
		mask.Set(info->tuples[i], info_mask.RowIsValidUnsafe(info->tuples[i]));
	}
}

static NumericSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type) {
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
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

} // namespace duckdb
