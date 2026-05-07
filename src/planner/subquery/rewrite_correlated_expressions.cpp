#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

RewriteCorrelatedExpressions::RewriteCorrelatedExpressions(column_binding_map_t<ColumnBinding> current_binding_map,
                                                           column_binding_map_t<ColumnBinding> &correlated_aliases)
    : current_binding_map(std::move(current_binding_map)), correlated_aliases(correlated_aliases) {
}

void RewriteCorrelatedExpressions::Rewrite(LogicalOperator &op, column_binding_map_t<ColumnBinding> current_binding_map,
                                           column_binding_map_t<ColumnBinding> &correlated_aliases) {
	RewriteCorrelatedExpressions rewriter(std::move(current_binding_map), correlated_aliases);
	rewriter.VisitOperator(op);
}

void RewriteCorrelatedExpressions::RegisterCorrelatedBinding(const ColumnBinding &source_binding,
                                                             const ColumnBinding &target_binding) {
	auto source_entry = correlated_aliases.find(source_binding);
	D_ASSERT(source_entry != correlated_aliases.end());
	auto result = correlated_aliases.emplace(target_binding, source_entry->second);
	D_ASSERT(result.second || result.first->second == source_entry->second);
}

void RewriteCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
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
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (expr.depth == 0) {
		return nullptr;
	}
	auto alias_entry = correlated_aliases.find(expr.binding);
	if (alias_entry == correlated_aliases.end()) {
		return nullptr;
	}
	auto current_entry = current_binding_map.find(alias_entry->second);
	D_ASSERT(current_entry != current_binding_map.end());
	auto original_binding = expr.binding;
	expr.binding = current_entry->second;
	RegisterCorrelatedBinding(original_binding, expr.binding);
	D_ASSERT(expr.depth > 0);
	expr.depth--;
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
	auto entry = replacement_map.find(expr.binding);
	if (entry != replacement_map.end()) {
		// reference to a COUNT(*) aggregate
		// replace this with CASE WHEN COUNT(*) IS NULL THEN 0 ELSE COUNT(*) END
		auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		is_null->children.push_back(expr.Copy());
		auto check = std::move(is_null);
		auto result_if_true = make_uniq<BoundConstantExpression>(Value::Numeric(expr.GetReturnType(), 0));
		auto result_if_false = std::move(*expr_ptr);
		return make_uniq<BoundCaseExpression>(std::move(check), std::move(result_if_true), std::move(result_if_false));
	}
	return nullptr;
}

} // namespace duckdb
