#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_binder/lateral_binder.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/subquery/column_binding_layout.hpp"
#include "duckdb/planner/subquery/pair_dependent_full_outer_join.hpp"
#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> RecursiveDependentJoinPlanner::PlanPairDependentLateralJoin(
    Binder &binder, unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
    unique_ptr<Expression> condition, const unordered_set<TableIndex> &left_bindings, JoinType join_type) {
	auto filter = make_uniq<LogicalFilter>(std::move(condition));
	filter->AddChild(std::move(right));
	right = std::move(filter);
	auto correlated_columns = LateralBinder::InsertLateralScope(*right, left_bindings);
	if (correlated_columns.empty()) {
		throw InternalException("Pair-dependent join condition did not produce lateral correlations");
	}
	return binder.PlanLateralJoin(std::move(left), std::move(right), correlated_columns, join_type, nullptr);
}

static bool HasPairDependentSubquery(Expression &expression, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY &&
	    JoinSide::GetCurrentJoinSide(expression, left_bindings, right_bindings) == JoinSide::BOTH) {
		return true;
	}
	bool result = false;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		result = result || HasPairDependentSubquery(child, left_bindings, right_bindings);
	});
	return result;
}

static bool SupportsPairDependentRewrite(JoinType join_type) {
	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
	case JoinType::OUTER:
		return true;
	default:
		return false;
	}
}

bool RecursiveDependentJoinPlanner::CanRewritePairDependentJoinCondition(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN || op.children.size() != 2) {
		return false;
	}
	auto &join = op.Cast<LogicalDependentJoin>();
	if (join.dependent_type != DependentJoinType::JOIN_CONDITION || !join.condition ||
	    !SupportsPairDependentRewrite(join.join_type)) {
		return false;
	}
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op.children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op.children[1], right_bindings);
	return HasPairDependentSubquery(*join.condition, left_bindings, right_bindings);
}

static unique_ptr<LogicalOperator> RestoreJoinOutput(Binder &binder, unique_ptr<LogicalOperator> plan,
                                                     const vector<ColumnBinding> &expected_bindings,
                                                     vector<ReplacementBinding> &replacements) {
	plan->ResolveOperatorTypes();
	auto output = ColumnBindingLayout(plan->GetColumnBindings(), "normalized pair-dependent join output");
	if (output.HasSameLayout(expected_bindings)) {
		return plan;
	}
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(expected_bindings.size());
	for (auto &binding : expected_bindings) {
		auto position = output.GetPosition(binding);
		expressions.push_back(make_uniq<BoundColumnRefExpression>(plan->types[position], binding));
	}
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(plan));
	auto projection_bindings = projection->GetColumnBindings();
	for (idx_t i = 0; i < expected_bindings.size(); i++) {
		replacements.emplace_back(expected_bindings[i], projection_bindings[i]);
	}
	return std::move(projection);
}

bool RecursiveDependentJoinPlanner::TryRewritePairDependentJoinCondition(Binder &binder,
                                                                         unique_ptr<LogicalOperator> &op,
                                                                         vector<ReplacementBinding> &replacements) {
	if (!op || !CanRewritePairDependentJoinCondition(*op)) {
		return false;
	}

	auto &join = op->Cast<LogicalDependentJoin>();
	auto join_type = join.join_type;
	auto expected_bindings = op->GetColumnBindings();
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
	if (join_type == JoinType::OUTER) {
		op->children[0]->ResolveOperatorTypes();
		op->children[1]->ResolveOperatorTypes();
		if (op->children[0]->GetColumnBindings().empty() || op->children[1]->GetColumnBindings().empty()) {
			return false;
		}
	}

	auto condition = std::move(join.condition);
	auto left = std::move(op->children[0]);
	auto right = std::move(op->children[1]);

	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::SEMI:
	case JoinType::ANTI:
		op = PlanPairDependentLateralJoin(binder, std::move(left), std::move(right), std::move(condition),
		                                  left_bindings, join_type);
		return true;
	case JoinType::RIGHT:
		op = PlanPairDependentLateralJoin(binder, std::move(right), std::move(left), std::move(condition),
		                                  right_bindings, JoinType::LEFT);
		op = RestoreJoinOutput(binder, std::move(op), expected_bindings, replacements);
		return true;
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI: {
		auto normalized_type = join_type == JoinType::RIGHT_SEMI ? JoinType::SEMI : JoinType::ANTI;
		op = PlanPairDependentLateralJoin(binder, std::move(right), std::move(left), std::move(condition),
		                                  right_bindings, normalized_type);
		return true;
	}
	case JoinType::OUTER: {
		auto result = PairDependentFullOuterJoinPlanner::Plan(binder, std::move(condition), std::move(left),
		                                                      std::move(right), left_bindings, right_bindings);
		op = std::move(result.plan);
		replacements = std::move(result.output_replacements);
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
