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

RewriteCorrelatedExpressions::RewriteCorrelatedExpressions(
    vector<ColumnBinding> correlated_bindings, column_binding_map_t<idx_t> &correlated_map,
    optional_ptr<column_binding_map_t<ColumnBinding>> equivalent_bindings)
    : correlated_bindings(std::move(correlated_bindings)), correlated_map(correlated_map),
      equivalent_bindings(equivalent_bindings) {
}

void RewriteCorrelatedExpressions::Rewrite(LogicalOperator &op, vector<ColumnBinding> correlated_bindings,
                                           column_binding_map_t<idx_t> &correlated_map,
                                           optional_ptr<column_binding_map_t<ColumnBinding>> equivalent_bindings) {
	RewriteCorrelatedExpressions rewriter(std::move(correlated_bindings), correlated_map, equivalent_bindings);
	rewriter.VisitOperator(op);
}

void RewriteCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	// update the bindings in the correlated columns of the dependent join
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &plan = op.Cast<LogicalDependentJoin>();
		for (auto &corr : plan.correlated_columns) {
			auto original_binding = corr.binding;
			auto entry = correlated_map.find(corr.binding);
			if (entry != correlated_map.end()) {
				D_ASSERT(entry->second < correlated_bindings.size());
				corr.binding = correlated_bindings[entry->second];
				if (equivalent_bindings && original_binding != corr.binding) {
					auto canonical_entry = equivalent_bindings->find(original_binding);
					auto canonical_binding =
					    canonical_entry == equivalent_bindings->end() ? original_binding : canonical_entry->second;
					(*equivalent_bindings)[corr.binding] = canonical_binding;
				}
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
	auto entry = correlated_map.find(expr.binding);
	if (entry == correlated_map.end()) {
		return nullptr;
	}
	D_ASSERT(entry->second < correlated_bindings.size());

	expr.binding = correlated_bindings[entry->second];
	D_ASSERT(expr.depth > 0);
	expr.depth--;
	return nullptr;
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundSubqueryExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (!expr.IsCorrelated()) {
		return nullptr;
	}
	Rewrite(*expr.subquery.plan, correlated_bindings, correlated_map, equivalent_bindings);
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
		auto result_if_true = make_uniq<BoundConstantExpression>(Value::Numeric(expr.return_type, 0));
		auto result_if_false = std::move(*expr_ptr);
		return make_uniq<BoundCaseExpression>(std::move(check), std::move(result_if_true), std::move(result_if_false));
	}
	return nullptr;
}

} // namespace duckdb
