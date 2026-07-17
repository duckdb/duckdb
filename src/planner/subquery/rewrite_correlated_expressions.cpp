#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

static bool ReclassifyJoinConditions(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	bool reclassified = false;
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op.children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op.children[1], right_bindings);
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			continue;
		}
		auto left_side = JoinSide::GetCurrentJoinSide(condition.GetLHS(), left_bindings, right_bindings);
		auto right_side = JoinSide::GetCurrentJoinSide(condition.GetRHS(), left_bindings, right_bindings);
		if (left_side != JoinSide::BOTH && right_side != JoinSide::BOTH) {
			continue;
		}
		auto expression = JoinCondition::CreateExpression(std::move(condition));
		condition = JoinCondition(std::move(expression));
		reclassified = true;
	}
	return reclassified;
}

static void NormalizeComparisonJoin(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &join = op->Cast<LogicalComparisonJoin>();
	for (auto &condition : join.conditions) {
		if (condition.IsComparison()) {
			return;
		}
	}

	D_ASSERT(join.mark_types.empty());
	D_ASSERT(join.duplicate_eliminated_columns.empty());
	D_ASSERT(!join.filter_pushdown);
	auto any_join = make_uniq<LogicalAnyJoin>(join.join_type);
	any_join->condition = JoinCondition::CreateExpression(std::move(join.conditions));
	if (!any_join->condition) {
		any_join->condition = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	}
	any_join->mark_index = join.mark_index;
	any_join->left_projection_map = std::move(join.left_projection_map);
	any_join->right_projection_map = std::move(join.right_projection_map);
	any_join->children = std::move(join.children);
	if (op->has_estimated_cardinality) {
		any_join->SetEstimatedCardinality(op->estimated_cardinality);
	}
	op = std::move(any_join);
}

RewriteCorrelatedExpressions::RewriteCorrelatedExpressions(column_binding_map_t<ColumnBinding> current_binding_map,
                                                           column_binding_map_t<ColumnBinding> &correlated_aliases)
    : current_binding_map(std::move(current_binding_map)), correlated_aliases(correlated_aliases) {
}

void RewriteCorrelatedExpressions::Rewrite(unique_ptr<LogicalOperator> &op,
                                           column_binding_map_t<ColumnBinding> current_binding_map,
                                           column_binding_map_t<ColumnBinding> &correlated_aliases) {
	RewriteCorrelatedExpressions rewriter(std::move(current_binding_map), correlated_aliases);
	rewriter.VisitOperator(op);
}

void RewriteCorrelatedExpressions::Rewrite(LogicalDependentJoin &op,
                                           column_binding_map_t<ColumnBinding> current_binding_map,
                                           column_binding_map_t<ColumnBinding> &correlated_aliases) {
	RewriteCorrelatedExpressions rewriter(std::move(current_binding_map), correlated_aliases);
	rewriter.RewriteOperator(op);
}

void RewriteCorrelatedExpressions::VisitOperator(unique_ptr<LogicalOperator> &op) {
	if (RewriteOperator(*op)) {
		NormalizeComparisonJoin(op);
	}
}

void RewriteCorrelatedExpressions::RegisterCorrelatedBinding(const ColumnBinding &source_binding,
                                                             const ColumnBinding &target_binding) {
	auto source_entry = correlated_aliases.find(source_binding);
	D_ASSERT(source_entry != correlated_aliases.end());
	auto result = correlated_aliases.emplace(target_binding, source_entry->second);
	if (!result.second) {
		D_ASSERT(result.first->second == source_entry->second);
	}
}

bool RewriteCorrelatedExpressions::RewriteOperator(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		VisitOperatorChildren(op);
	}
	// update the bindings in the correlated columns of the dependent join
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &plan = op.Cast<LogicalDependentJoin>();
		for (auto &corr : plan.correlated_columns) {
			auto alias_entry = correlated_aliases.find(corr.binding);
			if (alias_entry != correlated_aliases.end()) {
				auto current_entry = current_binding_map.find(alias_entry->second);
				D_ASSERT(current_entry != current_binding_map.end());
				auto original_binding = corr.binding;
				corr.binding = current_entry->second;
				RegisterCorrelatedBinding(original_binding, corr.binding);
			}
		}
	}
	VisitOperatorExpressions(op);
	return ReclassifyJoinConditions(op);
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (expr.Depth() == 0) {
		return nullptr;
	}
	auto alias_entry = correlated_aliases.find(expr.Binding());
	if (alias_entry == correlated_aliases.end()) {
		return nullptr;
	}
	auto current_entry = current_binding_map.find(alias_entry->second);
	D_ASSERT(current_entry != current_binding_map.end());
	auto original_binding = expr.Binding();
	if (original_binding == current_entry->second) {
		expr.DepthMutable() = 0;
		return nullptr;
	}
	expr.BindingMutable() = current_entry->second;
	RegisterCorrelatedBinding(original_binding, expr.Binding());
	// The current representative is produced inside the converted algebra. Once a
	// reference points at it, it is local regardless of the original binder depth.
	expr.DepthMutable() = 0;
	return nullptr;
}

RewriteCountAggregates::RewriteCountAggregates(column_binding_map_t<idx_t> &replacement_map)
    : replacement_map(replacement_map) {
}

void RewriteCountAggregates::Rewrite(LogicalOperator &op, column_binding_map_t<idx_t> &replacement_map) {
	RewriteCountAggregates rewriter(replacement_map);
	rewriter.VisitOperator(op);
}

unique_ptr<Expression> RewriteCountAggregates::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	auto entry = replacement_map.find(expr.Binding());
	if (entry != replacement_map.end()) {
		// reference to a COUNT(*) aggregate
		// replace this with CASE WHEN COUNT(*) IS NULL THEN 0 ELSE COUNT(*) END
		auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		is_null->GetChildrenMutable().push_back(expr.Copy());
		auto check = std::move(is_null);
		auto result_if_true = make_uniq<BoundConstantExpression>(Value::Numeric(expr.GetReturnType(), 0));
		auto result_if_false = std::move(*expr_ptr);
		return make_uniq<BoundCaseExpression>(std::move(check), std::move(result_if_true), std::move(result_if_false));
	}
	return nullptr;
}

} // namespace duckdb
