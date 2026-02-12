#include "duckdb/execution/operator/join/physical_comparison_join.hpp"

#include "duckdb/common/enum_util.hpp"

namespace duckdb {

PhysicalComparisonJoin::PhysicalComparisonJoin(PhysicalPlan &physical_plan, LogicalOperator &op,
                                               PhysicalOperatorType type, vector<JoinCondition> conditions_p,
                                               JoinType join_type, idx_t estimated_cardinality)
    : PhysicalJoin(physical_plan, op, type, join_type, estimated_cardinality) {
	vector<JoinCondition> comparison_conditions;
	vector<JoinCondition> arbitrary_conditions;

	for (auto &cond : conditions_p) {
		if (cond.IsComparison()) {
			comparison_conditions.push_back(std::move(cond));
		} else {
			arbitrary_conditions.push_back(std::move(cond));
		}
	}
	conditions = std::move(comparison_conditions);
	ReorderConditions(conditions);

	if (!arbitrary_conditions.empty()) {
		predicate = JoinCondition::CreateExpression(std::move(arbitrary_conditions));
	}
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
		D_ASSERT(join_condition.IsComparison());
		condition_info += StringUtil::Format("%s %s %s", join_condition.GetLHS().GetName(),
		                                     ExpressionTypeToOperator(join_condition.GetComparisonType()),
		                                     join_condition.GetRHS().GetName());
	}

	if (predicate) {
		if (!condition_info.empty()) {
			condition_info += "\n";
		}
		condition_info += predicate->ToString();
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
	bool seen_non_comparison = false;

	for (auto &cond : conditions) {
		if (!cond.IsComparison()) {
			seen_non_comparison = true;
		} else if (cond.GetComparisonType() == ExpressionType::COMPARE_EQUAL ||
		           cond.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			if (seen_non_equal || seen_non_comparison) {
				is_ordered = false;
				break;
			}
		} else {
			if (seen_non_comparison) {
				is_ordered = false;
				break;
			}
			seen_non_equal = true;
		}
	}

	if (is_ordered) {
		// no need to re-order
		return;
	}

	vector<JoinCondition> equal_conditions;
	vector<JoinCondition> non_equi_conditions;
	vector<JoinCondition> arbitrary_conditions;

	for (auto &cond : conditions) {
		if (!cond.IsComparison()) {
			arbitrary_conditions.push_back(std::move(cond));
		} else if (cond.GetComparisonType() == ExpressionType::COMPARE_EQUAL ||
		           cond.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			equal_conditions.push_back(std::move(cond));
		} else {
			non_equi_conditions.push_back(std::move(cond));
		}
	}

	conditions.clear();
	// reconstruct the sorted conditions
	for (auto &cond : equal_conditions) {
		conditions.push_back(std::move(cond));
	}
	for (auto &cond : non_equi_conditions) {
		conditions.push_back(std::move(cond));
	}
	for (auto &cond : arbitrary_conditions) {
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
