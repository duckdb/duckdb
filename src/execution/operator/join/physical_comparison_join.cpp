#include "duckdb/execution/operator/join/physical_comparison_join.hpp"

#include "duckdb/common/enum_util.hpp"

namespace duckdb {

PhysicalComparisonJoin::PhysicalComparisonJoin(LogicalOperator &op, PhysicalOperatorType type,
                                               vector<JoinCondition> conditions_p, JoinType join_type,
                                               idx_t estimated_cardinality)
    : PhysicalJoin(op, type, join_type, estimated_cardinality), conditions(std::move(conditions_p)) {
	ReorderConditions(conditions);
}

InsertionOrderPreservingMap<string> PhysicalComparisonJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToString(join_type);
	string condition_info;
	for (idx_t i = 0; i < conditions.size(); i++) {
		auto &join_condition = conditions[i];
		if (i > 0) {
			condition_info += "\n";
		}
		condition_info +=
		    StringUtil::Format("%s %s %s", join_condition.left->GetName(),
		                       ExpressionTypeToOperator(join_condition.comparison), join_condition.right->GetName());
		// string op = ExpressionTypeToOperator(it.comparison);
		// extra_info += it.left->GetName() + " " + op + " " + it.right->GetName() + "\n";
	}
	result["Conditions"] = condition_info;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

void PhysicalComparisonJoin::ReorderConditions(vector<JoinCondition> &conditions) {
	// we reorder conditions so the ones with COMPARE_EQUAL occur first
	// check if this is already the case
	bool is_ordered = true;
	bool seen_non_equal = false;
	for (auto &cond : conditions) {
		if (cond.comparison == ExpressionType::COMPARE_EQUAL ||
		    cond.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			if (seen_non_equal) {
				is_ordered = false;
				break;
			}
		} else {
			seen_non_equal = true;
		}
	}
	if (is_ordered) {
		// no need to re-order
		return;
	}
	// gather lists of equal/other conditions
	vector<JoinCondition> equal_conditions;
	vector<JoinCondition> other_conditions;
	for (auto &cond : conditions) {
		if (cond.comparison == ExpressionType::COMPARE_EQUAL ||
		    cond.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			equal_conditions.push_back(std::move(cond));
		} else {
			other_conditions.push_back(std::move(cond));
		}
	}
	conditions.clear();
	// reconstruct the sorted conditions
	for (auto &cond : equal_conditions) {
		conditions.push_back(std::move(cond));
	}
	for (auto &cond : other_conditions) {
		conditions.push_back(std::move(cond));
	}
}

void PhysicalComparisonJoin::ConstructEmptyJoinResult(JoinType join_type, bool has_null, DataChunk &input,
                                                      DataChunk &result) {
	// empty hash table, special case
	if (join_type == JoinType::ANTI) {
		// anti join with empty hash table, NOP join
		// return the input
		D_ASSERT(input.ColumnCount() == result.ColumnCount());
		result.Reference(input);
	} else if (join_type == JoinType::MARK) {
		// MARK join with empty hash table
		D_ASSERT(join_type == JoinType::MARK);
		D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
		auto &result_vector = result.data.back();
		D_ASSERT(result_vector.GetType() == LogicalType::BOOLEAN);
		// for every data vector, we just reference the child chunk
		result.SetCardinality(input);
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the MARK vector:
		// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
		// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
		// has NULL for every input entry
		if (!has_null) {
			auto bool_result = FlatVector::GetData<bool>(result_vector);
			for (idx_t i = 0; i < result.size(); i++) {
				bool_result[i] = false;
			}
		} else {
			FlatVector::Validity(result_vector).SetAllInvalid(result.size());
		}
	} else if (join_type == JoinType::LEFT || join_type == JoinType::OUTER || join_type == JoinType::SINGLE) {
		// LEFT/FULL OUTER/SINGLE join and build side is empty
		// for the LHS we reference the data
		result.SetCardinality(input.size());
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the RHS
		for (idx_t k = input.ColumnCount(); k < result.ColumnCount(); k++) {
			result.data[k].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[k], true);
		}
	}
}

} // namespace duckdb
