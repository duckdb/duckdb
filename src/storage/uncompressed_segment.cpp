#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

UncompressedSegment::UncompressedSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start)
    : db(db), type(type), max_vector_count(0), tuple_count(0), row_start(row_start) {
}

UncompressedSegment::~UncompressedSegment() {
}

void UncompressedSegment::Verify() {
#ifdef DEBUG
	// ColumnScanState state;
	// InitializeScan(state);

	// Vector result(this->type);
	// for (idx_t i = 0; i < this->tuple_count; i += STANDARD_VECTOR_SIZE) {
	// 	idx_t vector_idx = i / STANDARD_VECTOR_SIZE;
	// 	idx_t count = MinValue((idx_t)STANDARD_VECTOR_SIZE, tuple_count - i);
	// 	Scan(transaction, state, vector_idx, result);
	// 	result.Verify(count);
	// }
#endif
}

void UncompressedSegment::Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) {
	InitializeScan(state);
	FetchBaseData(state, vector_index, result);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void UncompressedSegment::Scan(ColumnScanState &state, idx_t vector_index, Vector &result) {
	FetchBaseData(state, vector_index, result);
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

void UncompressedSegment::FilterSelection(SelectionVector &sel, Vector &result, const ConstantFilter &filter,
                                          idx_t &approved_tuple_count, ValidityMask &mask) {
	// the inplace loops take the result as the last parameter
	switch (result.GetType().InternalType()) {
	case PhysicalType::UINT8: {
		auto result_flat = FlatVector::GetData<uint8_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<uint8_t>(predicate_vector);
		FilterSelectionSwitch<uint8_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	case PhysicalType::UINT16: {
		auto result_flat = FlatVector::GetData<uint16_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<uint16_t>(predicate_vector);
		FilterSelectionSwitch<uint16_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
		                                mask);
		break;
	}
	case PhysicalType::UINT32: {
		auto result_flat = FlatVector::GetData<uint32_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<uint32_t>(predicate_vector);
		FilterSelectionSwitch<uint32_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
		                                mask);
		break;
	}
	case PhysicalType::UINT64: {
		auto result_flat = FlatVector::GetData<uint64_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<uint64_t>(predicate_vector);
		FilterSelectionSwitch<uint64_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
		                                mask);
		break;
	}
	case PhysicalType::INT8: {
		auto result_flat = FlatVector::GetData<int8_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<int8_t>(predicate_vector);
		FilterSelectionSwitch<int8_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	case PhysicalType::INT16: {
		auto result_flat = FlatVector::GetData<int16_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<int16_t>(predicate_vector);
		FilterSelectionSwitch<int16_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	case PhysicalType::INT32: {
		auto result_flat = FlatVector::GetData<int32_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<int32_t>(predicate_vector);
		FilterSelectionSwitch<int32_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	case PhysicalType::INT64: {
		auto result_flat = FlatVector::GetData<int64_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<int64_t>(predicate_vector);
		FilterSelectionSwitch<int64_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	case PhysicalType::INT128: {
		auto result_flat = FlatVector::GetData<hugeint_t>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<hugeint_t>(predicate_vector);
		FilterSelectionSwitch<hugeint_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
		                                 mask);
		break;
	}
	case PhysicalType::FLOAT: {
		auto result_flat = FlatVector::GetData<float>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<float>(predicate_vector);
		FilterSelectionSwitch<float>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	case PhysicalType::DOUBLE: {
		auto result_flat = FlatVector::GetData<double>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<double>(predicate_vector);
		FilterSelectionSwitch<double>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	case PhysicalType::VARCHAR: {
		auto result_flat = FlatVector::GetData<string_t>(result);
		Vector predicate_vector(filter.constant.str_value);
		auto predicate = FlatVector::GetData<string_t>(predicate_vector);
		FilterSelectionSwitch<string_t>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type,
		                                mask);
		break;
	}
	case PhysicalType::BOOL: {
		auto result_flat = FlatVector::GetData<bool>(result);
		Vector predicate_vector(filter.constant);
		auto predicate = FlatVector::GetData<bool>(predicate_vector);
		FilterSelectionSwitch<bool>(result_flat, predicate, sel, approved_tuple_count, filter.comparison_type, mask);
		break;
	}
	default:
		throw InvalidTypeException(result.GetType(), "Invalid type for filter pushed down to table comparison");
	}
}

void UncompressedSegment::RevertAppend(idx_t start_row) {
	tuple_count = start_row - this->row_start;
}

//===--------------------------------------------------------------------===//
// ToTemporary
//===--------------------------------------------------------------------===//
void UncompressedSegment::ToTemporary() {
	ToTemporaryInternal();
}

void UncompressedSegment::ToTemporaryInternal() {
	if (block->BlockId() >= MAXIMUM_BLOCK) {
		// conversion has already been performed by a different thread
		return;
	}
	auto &block_manager = BlockManager::GetBlockManager(db);
	block_manager.MarkBlockAsModified(block->BlockId());

	// pin the current block
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto current = buffer_manager.Pin(block);

	// now allocate a new block from the buffer manager
	auto new_block = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
	auto handle = buffer_manager.Pin(new_block);
	// now copy the data over and switch to using the new block id
	memcpy(handle->node->buffer, current->node->buffer, Storage::BLOCK_SIZE);
	this->block = move(new_block);
}

} // namespace duckdb
