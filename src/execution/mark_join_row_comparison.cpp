#include "duckdb/execution/mark_join_row_comparison.hpp"

#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

static void CompareRowEqualityInternal(const Vector &left, idx_t left_row, idx_t left_count, const Vector &right,
                                       idx_t right_count, const bool active[], bool row_is_false[],
                                       bool row_is_unknown[]) {
	if (left.GetType().id() != LogicalTypeId::TUPLE) {
		Vector left_reference(left.GetType());
		ConstantVector::Reference(left_reference, count_t(right_count), left, left_row, left_count);
		Vector comparison(LogicalType::BOOLEAN, right_count);
		VectorOperations::Equals(left_reference, right, comparison);

		UnifiedVectorFormat comparison_format;
		comparison.ToUnifiedFormat(comparison_format);
		auto comparison_data = comparison_format.GetData<bool>();
		for (idx_t right_row = 0; right_row < right_count; right_row++) {
			if (!active[right_row] || row_is_false[right_row]) {
				continue;
			}
			auto comparison_idx = comparison_format.sel->get_index(right_row);
			if (!comparison_format.validity.RowIsValid(comparison_idx)) {
				row_is_unknown[right_row] = true;
			} else if (!comparison_data[comparison_idx]) {
				row_is_false[right_row] = true;
				row_is_unknown[right_row] = false;
			}
		}
		return;
	}

	D_ASSERT(right.GetType().id() == LogicalTypeId::TUPLE);
	UnifiedVectorFormat left_format;
	UnifiedVectorFormat right_format;
	left.ToUnifiedFormat(left_format);
	right.ToUnifiedFormat(right_format);
	auto left_idx = left_format.sel->get_index(left_row);

	bool child_active[STANDARD_VECTOR_SIZE] = {false};
	for (idx_t right_row = 0; right_row < right_count; right_row++) {
		if (!active[right_row] || row_is_false[right_row]) {
			continue;
		}
		auto right_idx = right_format.sel->get_index(right_row);
		if (!left_format.validity.RowIsValid(left_idx) || !right_format.validity.RowIsValid(right_idx)) {
			row_is_unknown[right_row] = true;
		} else {
			child_active[right_row] = true;
		}
	}

	auto &left_children = StructVector::GetEntries(left);
	auto &right_children = StructVector::GetEntries(right);
	D_ASSERT(left_children.size() == right_children.size());
	for (idx_t child_idx = 0; child_idx < left_children.size(); child_idx++) {
		CompareRowEqualityInternal(left_children[child_idx], left_row, left_count, right_children[child_idx],
		                           right_count, child_active, row_is_false, row_is_unknown);
	}
}

void MarkJoinRowComparison::CompareEquality(const Vector &left, idx_t left_row, idx_t left_count, const Vector &right,
                                            idx_t right_count, bool row_is_false[], bool row_is_unknown[]) {
	D_ASSERT(left.GetType() == right.GetType());
	D_ASSERT(right_count <= STANDARD_VECTOR_SIZE);
	bool active[STANDARD_VECTOR_SIZE];
	for (idx_t right_row = 0; right_row < right_count; right_row++) {
		active[right_row] = !row_is_false[right_row];
	}
	CompareRowEqualityInternal(left, left_row, left_count, right, right_count, active, row_is_false, row_is_unknown);
	for (idx_t right_row = 0; right_row < right_count; right_row++) {
		D_ASSERT(!row_is_false[right_row] || !row_is_unknown[right_row]);
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

MarkJoinRowComparison::MarkJoinRowComparison(const DataChunk &left) : comparison(LogicalType::BOOLEAN) {
	left_reference.Initialize(Allocator::DefaultAllocator(), left.GetTypes());
}

void MarkJoinRowComparison::CompareConjunction(DataChunk &left, idx_t left_row, DataChunk &right,
                                               const vector<JoinCondition> &conditions, Vector &result) {
	D_ASSERT(left_row < left.size());
	D_ASSERT(right.size() <= STANDARD_VECTOR_SIZE);
	D_ASSERT(left.ColumnCount() == conditions.size());
	D_ASSERT(right.ColumnCount() == conditions.size());
	left_reference.Reset();
	bool pair_is_false[STANDARD_VECTOR_SIZE] = {false};
	bool pair_is_unknown[STANDARD_VECTOR_SIZE] = {false};
	for (idx_t condition_idx = 0; condition_idx < conditions.size(); condition_idx++) {
		const auto type = conditions[condition_idx].GetComparisonType();
		if (left.data[condition_idx].GetType().id() == LogicalTypeId::TUPLE &&
		    (type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_NOTEQUAL)) {
			bool is_false[STANDARD_VECTOR_SIZE] = {false};
			bool is_unknown[STANDARD_VECTOR_SIZE] = {false};
			CompareEquality(left.data[condition_idx], left_row, left.size(), right.data[condition_idx], right.size(),
			                is_false, is_unknown);
			comparison.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::ValidityMutable(comparison).Reset(right.size());
			auto writer = FlatVector::Writer<bool>(comparison, right.size());
			for (idx_t row = 0; row < right.size(); row++) {
				if (is_unknown[row]) {
					writer.WriteNull();
				} else {
					writer.WriteValue(type == ExpressionType::COMPARE_EQUAL ? !is_false[row] : is_false[row]);
				}
			}
		} else {
			ConstantVector::Reference(left_reference.data[condition_idx], count_t(right.size()),
			                          left.data[condition_idx], left_row, left.size());
			Compare(left_reference.data[condition_idx], right.data[condition_idx], type, comparison);
		}
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
	MarkJoinRowComparison comparer(left);
	for (idx_t left_row = 0; left_row < left.size(); left_row++) {
		if (found_match[left_row]) {
			continue;
		}
		comparer.CompareConjunction(left, left_row, right, conditions, comparison);
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
