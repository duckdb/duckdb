#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/joinside.hpp"

namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

InsertionOrderPreservingMap<string> LogicalComparisonJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToChars(join_type);

	string conditions_info;
	for (idx_t i = 0; i < conditions.size(); i++) {
		if (i > 0) {
			conditions_info += "\n";
		}
		auto &condition = conditions[i];
		if (condition.IsComparison()) {
			auto expr = BoundComparisonExpression::Create(condition.GetComparisonType(), condition.GetLHS().Copy(),
			                                              condition.GetRHS().Copy());
			conditions_info += expr->ToString();
		} else {
			conditions_info += condition.GetJoinExpression().ToString();
		}
	}
	result["Conditions"] = conditions_info;
	SetParamsEstimatedCardinality(result);

	return result;
}

bool LogicalComparisonJoin::HasEquality(idx_t &range_count) const {
	bool result = false;
	for (size_t c = 0; c < conditions.size(); ++c) {
		auto &cond = conditions[c];
		if (cond.IsComparison()) {
			switch (cond.GetComparisonType()) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				result = true;
				break;
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				++range_count;
				break;
			case ExpressionType::COMPARE_NOTEQUAL:
			case ExpressionType::COMPARE_DISTINCT_FROM:
				break;
			default:
				throw NotImplementedException("Unimplemented comparison join");
			}
		}
	}
	return result;
}

bool LogicalComparisonJoin::HasArbitraryConditions() const {
	for (size_t c = 0; c < conditions.size(); ++c) {
		auto &cond = conditions[c];
		if (!cond.IsComparison()) {
			return true;
		}
	}
	return false;
}

bool LogicalComparisonJoin::ConditionsAreCanonical(LogicalComparisonJoin &join) {
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*join.children[0], left_bindings);
	LogicalJoin::GetTableReferences(*join.children[1], right_bindings);
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			continue;
		}
		auto left_side = JoinSide::GetCurrentJoinSide(condition.GetLHS(), left_bindings, right_bindings);
		auto right_side = JoinSide::GetCurrentJoinSide(condition.GetRHS(), left_bindings, right_bindings);
		// NONE represents an external reference that is still owned by an enclosing dependent join.
		if (left_side == JoinSide::BOTH || right_side == JoinSide::BOTH || left_side == JoinSide::RIGHT ||
		    right_side == JoinSide::LEFT) {
			return false;
		}
	}
	return true;
}

void LogicalComparisonJoin::FinalizeBindingRewrite(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	if (plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!ConditionsAreCanonical(plan->Cast<LogicalComparisonJoin>())) {
			throw InternalException("Binding rewrite changed condition ownership in a DELIM_JOIN");
		}
		return;
	}
	if (plan->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &join = plan->Cast<LogicalComparisonJoin>();
	if (ConditionsAreCanonical(join)) {
		return;
	}

	if (!join.mark_types.empty() || !join.duplicate_eliminated_columns.empty() || join.delim_flipped ||
	    join.filter_pushdown) {
		throw InternalException("Cannot reclassify a comparison join with specialized join metadata");
	}
	auto join_type = join.join_type;
	auto convert_mark_to_semi = join.convert_mark_to_semi;
	auto condition = JoinCondition::CreateExpression(std::move(join.conditions));
	auto left = std::move(join.children[0]);
	auto right = std::move(join.children[1]);
	auto result =
	    CreateJoin(context, join_type, JoinRefType::REGULAR, std::move(left), std::move(right), std::move(condition));
	LogicalJoin::MoveJoinProperties(join, result->Cast<LogicalJoin>());
	if (result->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		result->Cast<LogicalComparisonJoin>().convert_mark_to_semi = convert_mark_to_semi;
		D_ASSERT(ConditionsAreCanonical(result->Cast<LogicalComparisonJoin>()));
	}
	plan = std::move(result);
}

} // namespace duckdb
