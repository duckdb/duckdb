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

NumericSegment::NumericSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start, block_id_t block_id)
    : UncompressedSegment(db, type, row_start) {
	// set up the different functions for this type of segment
	this->append_function = GetAppendFunction(type);

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
		result_mask.Copy(source_mask, STANDARD_VECTOR_SIZE);
	}

	auto source_data = data + offset + ValidityMask::STANDARD_MASK_SIZE;

	// fetch the nullmask and copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result), source_data, count * type_size);
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
void NumericSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
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
// Append
//===--------------------------------------------------------------------===//
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
				NumericStatistics::Update<T>(stats, sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			NumericStatistics::Update<T>(stats, sdata[source_idx]);
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

} // namespace duckdb
