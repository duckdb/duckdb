#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/segment/base_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

namespace duckdb {

BaseSegment::BaseSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start)
    : db(db), type(type), tuple_count(0), row_start(row_start) {
}

BaseSegment::~BaseSegment() {
}

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(T *vec, T *predicate, SelectionVector &sel, idx_t approved_tuple_count,
                                      ValidityMask &mask, SelectionVector &result_sel) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		auto idx = sel.get_index(i);
		if ((!HAS_NULL || mask.RowIsValid(idx)) && OP::Operation(vec[idx], *predicate)) {
			result_sel.set_index(result_count++, idx);
		}
	}
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(T *vec, T *predicate, SelectionVector &sel, idx_t &approved_tuple_count,
                                  ExpressionType comparison_type, ValidityMask &mask) {
	SelectionVector new_sel(approved_tuple_count);
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, false>(vec, predicate, sel,
			                                                                       approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, true>(vec, predicate, sel,
			                                                                      approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel.Initialize(new_sel);
}

template <bool IS_NULL>
static idx_t TemplatedNullSelection(SelectionVector &sel, idx_t approved_tuple_count, ValidityMask &mask) {
	if (mask.AllValid()) {
		// no NULL values
		if (IS_NULL) {
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		SelectionVector result_sel(approved_tuple_count);
		idx_t result_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			if (mask.RowIsValid(idx) != IS_NULL) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		return result_count;
	}
}

void BaseSegment::FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
                                          idx_t &approved_tuple_count, ValidityMask &mask) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = (ConjunctionAndFilter &)filter;
		for (auto &child_filter : conjunction_and.child_filters) {
			FilterSelection(sel, result, *child_filter, approved_tuple_count, mask);
		}
		break;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = (ConstantFilter &)filter;
		// the inplace loops take the result as the last parameter
		switch (result.GetType().InternalType()) {
		case PhysicalType::UINT8: {
			auto result_flat = FlatVector::GetData<uint8_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint8_t>(predicate_vector);
			FilterSelectionSwitch<uint8_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT16: {
			auto result_flat = FlatVector::GetData<uint16_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint16_t>(predicate_vector);
			FilterSelectionSwitch<uint16_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT32: {
			auto result_flat = FlatVector::GetData<uint32_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint32_t>(predicate_vector);
			FilterSelectionSwitch<uint32_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT64: {
			auto result_flat = FlatVector::GetData<uint64_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<uint64_t>(predicate_vector);
			FilterSelectionSwitch<uint64_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT8: {
			auto result_flat = FlatVector::GetData<int8_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int8_t>(predicate_vector);
			FilterSelectionSwitch<int8_t>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT16: {
			auto result_flat = FlatVector::GetData<int16_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int16_t>(predicate_vector);
			FilterSelectionSwitch<int16_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT32: {
			auto result_flat = FlatVector::GetData<int32_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int32_t>(predicate_vector);
			FilterSelectionSwitch<int32_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT64: {
			auto result_flat = FlatVector::GetData<int64_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<int64_t>(predicate_vector);
			FilterSelectionSwitch<int64_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT128: {
			auto result_flat = FlatVector::GetData<hugeint_t>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<hugeint_t>(predicate_vector);
			FilterSelectionSwitch<hugeint_t>(result_flat, predicate, sel, approved_tuple_count,
			                                 constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::FLOAT: {
			auto result_flat = FlatVector::GetData<float>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<float>(predicate_vector);
			FilterSelectionSwitch<float>(result_flat, predicate, sel, approved_tuple_count,
			                             constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::DOUBLE: {
			auto result_flat = FlatVector::GetData<double>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<double>(predicate_vector);
			FilterSelectionSwitch<double>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::VARCHAR: {
			auto result_flat = FlatVector::GetData<string_t>(result);
			Vector predicate_vector(constant_filter.constant.str_value);
			auto predicate = FlatVector::GetData<string_t>(predicate_vector);
			FilterSelectionSwitch<string_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::BOOL: {
			auto result_flat = FlatVector::GetData<bool>(result);
			Vector predicate_vector(constant_filter.constant);
			auto predicate = FlatVector::GetData<bool>(predicate_vector);
			FilterSelectionSwitch<bool>(result_flat, predicate, sel, approved_tuple_count,
			                            constant_filter.comparison_type, mask);
			break;
		}
		default:
			throw InvalidTypeException(result.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		break;
	}
	case TableFilterType::IS_NULL:
		TemplatedNullSelection<true>(sel, approved_tuple_count, mask);
		break;
	case TableFilterType::IS_NOT_NULL:
		TemplatedNullSelection<false>(sel, approved_tuple_count, mask);
		break;
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

bool BaseSegment::RowIdIsValid(idx_t row_id) const {
	return row_id <= tuple_count;
}

bool BaseSegment::RowRangeIsValid(idx_t row_id, idx_t count) const {
	return row_id <= tuple_count && row_id + count <= tuple_count;
}

idx_t BaseSegment::Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	throw InternalException("Unsupported type for Append");
}

void BaseSegment::RevertAppend(idx_t start_row) {
	tuple_count = start_row - this->row_start;
}

}
