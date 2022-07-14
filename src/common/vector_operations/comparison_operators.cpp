//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/likely.hpp"

namespace duckdb {

template <class T>
bool EqualsFloat(T left, T right) {
	if (DUCKDB_UNLIKELY(Value::IsNan(left) && Value::IsNan(right))) {
		return true;
	}
	return left == right;
}

template <>
bool Equals::Operation(float left, float right) {
	return EqualsFloat<float>(left, right);
}

template <>
bool Equals::Operation(double left, double right) {
	return EqualsFloat<double>(left, right);
}

template <class T>
bool GreaterThanFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no number that is bigger than right
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return false;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left > right;
}

template <>
bool GreaterThan::Operation(float left, float right) {
	return GreaterThanFloat<float>(left, right);
}

template <>
bool GreaterThan::Operation(double left, double right) {
	return GreaterThanFloat<double>(left, right);
}

template <class T>
bool GreaterThanEqualsFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no bigger number
	// we only return true if left is also nan (in which case the numbers are equal)
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return left_is_nan;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left >= right;
}

template <>
bool GreaterThanEquals::Operation(float left, float right) {
	return GreaterThanEqualsFloat<float>(left, right);
}

template <>
bool GreaterThanEquals::Operation(double left, double right) {
	return GreaterThanEqualsFloat<double>(left, right);
}

struct ComparisonSelector {
	template <typename OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel) {
		throw NotImplementedException("Unknown comparison operation!");
	}
};

template <>
inline idx_t ComparisonSelector::Select<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                        idx_t count, SelectionVector *true_sel,
                                                        SelectionVector *false_sel) {
	return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                           idx_t count, SelectionVector *true_sel,
                                                           SelectionVector *false_sel) {
	return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::GreaterThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                             idx_t count, SelectionVector *true_sel,
                                                             SelectionVector *false_sel) {
	return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::GreaterThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector *sel, idx_t count,
                                                                   SelectionVector *true_sel,
                                                                   SelectionVector *false_sel) {
	return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                          idx_t count, SelectionVector *true_sel,
                                                          SelectionVector *false_sel) {
	return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                                idx_t count, SelectionVector *true_sel,
                                                                SelectionVector *false_sel) {
	return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
}

static void ComparesNotNull(UnifiedVectorFormat &ldata, UnifiedVectorFormat &rdata, ValidityMask &vresult,
                            idx_t count) {
	for (idx_t i = 0; i < count; ++i) {
		auto lidx = ldata.sel->get_index(i);
		auto ridx = rdata.sel->get_index(i);
		if (!ldata.validity.RowIsValid(lidx) || !rdata.validity.RowIsValid(ridx)) {
			vresult.SetInvalid(i);
		}
	}
}

template <typename OP>
static void NestedComparisonExecutor(Vector &left, Vector &right, Vector &result, idx_t count) {
	const auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if ((left_constant && ConstantVector::IsNull(left)) || (right_constant && ConstantVector::IsNull(right))) {
		// either left or right is constant NULL: result is constant NULL
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	if (left_constant && right_constant) {
		// both sides are constant, and neither is NULL so just compare one element.
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		SelectionVector true_sel(1);
		auto match_count = ComparisonSelector::Select<OP>(left, right, nullptr, 1, &true_sel, nullptr);
		auto result_data = ConstantVector::GetData<bool>(result);
		result_data[0] = match_count > 0;
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat leftv, rightv;
	left.ToUnifiedFormat(count, leftv);
	right.ToUnifiedFormat(count, rightv);
	if (!leftv.validity.AllValid() || !rightv.validity.AllValid()) {
		ComparesNotNull(leftv, rightv, result_validity, count);
	}
	SelectionVector true_sel(count);
	SelectionVector false_sel(count);
	idx_t match_count = ComparisonSelector::Select<OP>(left, right, nullptr, count, &true_sel, &false_sel);

	for (idx_t i = 0; i < match_count; ++i) {
		const auto idx = true_sel.get_index(i);
		result_data[idx] = true;
	}

	const idx_t no_match_count = count - match_count;
	for (idx_t i = 0; i < no_match_count; ++i) {
		const auto idx = false_sel.get_index(i);
		result_data[idx] = false;
	}
}

struct ComparisonExecutor {
private:
	template <class T, class OP>
	static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::Execute<T, T, bool, OP>(left, right, result, count);
	}

public:
	template <class OP>
	static inline void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		D_ASSERT(left.GetType() == right.GetType() && result.GetType() == LogicalType::BOOLEAN);
		// the inplace loops take the result as the last parameter
		switch (left.GetType().InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedExecute<int8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT16:
			TemplatedExecute<int16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT32:
			TemplatedExecute<int32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT64:
			TemplatedExecute<int64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT8:
			TemplatedExecute<uint8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT16:
			TemplatedExecute<uint16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT32:
			TemplatedExecute<uint32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT64:
			TemplatedExecute<uint64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT128:
			TemplatedExecute<hugeint_t, OP>(left, right, result, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedExecute<float, OP>(left, right, result, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedExecute<double, OP>(left, right, result, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedExecute<interval_t, OP>(left, right, result, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedExecute<string_t, OP>(left, right, result, count);
			break;
		case PhysicalType::LIST:
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			NestedComparisonExecutor<OP>(left, right, result, count);
			break;
		default:
			throw InternalException("Invalid type for comparison");
		}
	}
};

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::Equals>(left, right, result, count);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::NotEquals>(left, right, result, count);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(left, right, result, count);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::LessThanEquals>(left, right, result, count);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(left, right, result, count);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::LessThan>(left, right, result, count);
}

} // namespace duckdb
