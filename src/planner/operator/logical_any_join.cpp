#include "duckdb/planner/operator/logical_any_join.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

LogicalAnyJoin::LogicalAnyJoin(JoinType type) : LogicalJoin(type, LogicalOperatorType::LOGICAL_ANY_JOIN) {
}

void LogicalAnyJoin::TrySpecialize(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_ANY_JOIN) {
		return;
	}
	auto &any_join = op->Cast<LogicalAnyJoin>();
	D_ASSERT(any_join.condition);
	if (any_join.condition->IsVolatile()) {
		return;
	}

	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*any_join.children[0], left_bindings);
	LogicalJoin::GetTableReferences(*any_join.children[1], right_bindings);

	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(any_join.condition));
	LogicalFilter::SplitPredicates(expressions);
	vector<JoinCondition> conditions;
	idx_t comparison_count = 0;
	for (auto &expression : expressions) {
		if (BoundComparisonExpression::IsComparison(*expression)) {
			auto &comparison = expression->Cast<BoundFunctionExpression>();
			auto left_side = JoinSide::GetCurrentJoinSide(BoundComparisonExpression::Left(comparison), left_bindings,
			                                              right_bindings);
			auto right_side = JoinSide::GetCurrentJoinSide(BoundComparisonExpression::Right(comparison), left_bindings,
			                                               right_bindings);
			if ((left_side == JoinSide::LEFT && right_side == JoinSide::RIGHT) ||
			    (left_side == JoinSide::RIGHT && right_side == JoinSide::LEFT)) {
				auto comparison_type = expression->GetExpressionType();
				auto left = std::move(BoundComparisonExpression::LeftMutable(comparison));
				auto right = std::move(BoundComparisonExpression::RightMutable(comparison));
				if (left_side == JoinSide::RIGHT) {
					std::swap(left, right);
					comparison_type = FlipComparisonExpression(comparison_type);
				}
				conditions.emplace_back(std::move(left), std::move(right), comparison_type);
				comparison_count++;
				continue;
			}
		}
		conditions.emplace_back(std::move(expression));
	}

	if (comparison_count == 0) {
		any_join.condition = JoinCondition::CreateExpression(std::move(conditions));
		return;
	}

	auto specialized =
	    LogicalComparisonJoin::CreateJoin(any_join.join_type, JoinRefType::REGULAR, std::move(any_join.children[0]),
	                                      std::move(any_join.children[1]), std::move(conditions));
	D_ASSERT(specialized->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
	LogicalJoin::MoveJoinState(any_join, specialized->Cast<LogicalJoin>());
	op = std::move(specialized);
}

InsertionOrderPreservingMap<string> LogicalAnyJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Condition"] = condition->ToString();
	SetParamsEstimatedCardinality(result);
	return result;
}

} // namespace duckdb
