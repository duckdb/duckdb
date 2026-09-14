#include "duckdb/execution/mark_join_row_comparison.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class T>
static Value MarkRangeExtreme(const Vector &key, bool maximum, idx_t &null_count) {
	auto values = key.Values<T>();
	idx_t best = DConstants::INVALID_INDEX;
	for (idx_t row = 0; row < values.size(); row++) {
		auto entry = values[row];
		if (!entry.IsValid()) {
			null_count++;
		} else if (best == DConstants::INVALID_INDEX ||
		           (maximum ? GreaterThan::Operation(entry.GetValue(), values[best].GetValue())
		                    : LessThan::Operation(entry.GetValue(), values[best].GetValue()))) {
			best = row;
		}
	}
	return best == DConstants::INVALID_INDEX ? Value(key.GetType()) : key.GetValue(best);
}

void MarkJoinRowComparison::UpdateRangeBound(const Vector &key, ExpressionType comparison, Value &bound,
                                             idx_t &null_count) {
	const bool maximum =
	    comparison == ExpressionType::COMPARE_LESSTHAN || comparison == ExpressionType::COMPARE_LESSTHANOREQUALTO;
	D_ASSERT(maximum || comparison == ExpressionType::COMPARE_GREATERTHAN ||
	         comparison == ExpressionType::COMPARE_GREATERTHANOREQUALTO);
	Value value;
	switch (key.GetType().InternalType()) {
#define MARK_RANGE_EXTREME(TYPE, CPP_TYPE)                                                                             \
	case PhysicalType::TYPE:                                                                                           \
		value = MarkRangeExtreme<CPP_TYPE>(key, maximum, null_count);                                                  \
		break;
		MARK_RANGE_EXTREME(BOOL, bool)
		MARK_RANGE_EXTREME(INT8, int8_t)
		MARK_RANGE_EXTREME(INT16, int16_t)
		MARK_RANGE_EXTREME(INT32, int32_t)
		MARK_RANGE_EXTREME(INT64, int64_t)
		MARK_RANGE_EXTREME(INT128, hugeint_t)
		MARK_RANGE_EXTREME(UINT8, uint8_t)
		MARK_RANGE_EXTREME(UINT16, uint16_t)
		MARK_RANGE_EXTREME(UINT32, uint32_t)
		MARK_RANGE_EXTREME(UINT64, uint64_t)
		MARK_RANGE_EXTREME(UINT128, uhugeint_t)
		MARK_RANGE_EXTREME(FLOAT, float)
		MARK_RANGE_EXTREME(DOUBLE, double)
		MARK_RANGE_EXTREME(VARCHAR, string_t)
		MARK_RANGE_EXTREME(INTERVAL, interval_t)
#undef MARK_RANGE_EXTREME
	default:
		for (idx_t row = 0; row < key.size(); row++) {
			auto entry = key.GetValue(row);
			if (entry.IsNull()) {
				null_count++;
			} else if (value.IsNull() || (maximum ? ValueOperations::GreaterThan(entry, value)
			                                      : ValueOperations::LessThan(entry, value))) {
				value = std::move(entry);
			}
		}
	}
	if (!value.IsNull() && (bound.IsNull() || (maximum ? ValueOperations::GreaterThan(value, bound)
	                                                   : ValueOperations::LessThan(value, bound)))) {
		bound = std::move(value);
	}
}

void MarkJoinRowComparison::Compare(const Vector &left, const Vector &right, ExpressionType comparison_type,
                                    Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::ValidityMutable(result).Reset(right.size());
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, result);
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, result);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, result);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, result);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, result);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, result);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, result);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::NotDistinctFrom(left, right, result);
	default:
		throw InternalException("Unsupported comparison type for MARK join");
	}
}

void MarkJoinRowComparison::Compare(const Vector &left, idx_t left_row, const Vector &right,
                                    ExpressionType comparison_type, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::ValidityMutable(result).Reset(right.size());
	Vector left_reference(left.GetType());
	ConstantVector::Reference(left_reference, count_t(right.size()), left, left_row, left.size());
	Compare(left_reference, right, comparison_type, result);
}

void MarkJoinRowComparison::CompareConjunction(DataChunk &left, idx_t left_row, DataChunk &right,
                                               const vector<JoinCondition> &conditions, Vector &result) {
	D_ASSERT(left_row < left.size());
	D_ASSERT(right.size() <= STANDARD_VECTOR_SIZE);
	D_ASSERT(left.ColumnCount() == conditions.size());
	D_ASSERT(right.ColumnCount() == conditions.size());
	Vector comparison(LogicalType::BOOLEAN);
	bool pair_is_false[STANDARD_VECTOR_SIZE] = {false};
	bool pair_is_unknown[STANDARD_VECTOR_SIZE] = {false};
	for (idx_t condition_idx = 0; condition_idx < conditions.size(); condition_idx++) {
		MarkJoinRowComparison::Compare(left.data[condition_idx], left_row, right.data[condition_idx],
		                               conditions[condition_idx].GetComparisonType(), comparison);
		auto entries = comparison.Values<bool>();
		for (idx_t right_row = 0; right_row < right.size(); right_row++) {
			auto entry = entries[right_row];
			if (!entry.IsValid()) {
				pair_is_unknown[right_row] = true;
			} else if (!entry.GetValue()) {
				pair_is_false[right_row] = true;
			}
		}
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::ValidityMutable(result).Reset(right.size());
	auto writer = FlatVector::Writer<bool>(result, right.size());
	for (idx_t right_row = 0; right_row < right.size(); right_row++) {
		if (pair_is_false[right_row]) {
			writer.WriteValue(false);
		} else if (pair_is_unknown[right_row]) {
			writer.WriteNull();
		} else {
			writer.WriteValue(true);
		}
	}
}

void MarkJoinRowComparison::Perform(DataChunk &left, DataChunk &right, bool found_match[],
                                    const vector<JoinCondition> &conditions, optional_ptr<bool> found_unknown) {
	Vector comparison(LogicalType::BOOLEAN);
	for (idx_t left_row = 0; left_row < left.size(); left_row++) {
		if (found_match[left_row]) {
			continue;
		}
		MarkJoinRowComparison::CompareConjunction(left, left_row, right, conditions, comparison);
		for (auto entry : comparison.Values<bool>()) {
			if (entry.IsValid()) {
				if (entry.GetValue()) {
					found_match[left_row] = true;
					break;
				}
			} else if (found_unknown) {
				found_unknown.get()[left_row] = true;
			}
		}
	}
}

} // namespace duckdb
