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

RewriteCorrelatedExpressions::RewriteCorrelatedExpressions(ColumnBinding base_binding,
                                                           column_binding_map_t<idx_t> &correlated_map,
                                                           idx_t lateral_depth, bool recursive_rewrite)
    : base_binding(base_binding), correlated_map(correlated_map), lateral_depth(lateral_depth),
      recursive_rewrite(recursive_rewrite) {
}

void RewriteCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	if (recursive_rewrite) {
		// Update column bindings from left child of lateral to right child
		if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			D_ASSERT(op.children.size() == 2);
			VisitOperator(*op.children[0]);
			lateral_depth++;
			VisitOperator(*op.children[1]);
			lateral_depth--;
		} else {
			VisitOperatorChildren(op);
		}
	}
	// update the bindings in the correlated columns of the dependendent join
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &plan = op.Cast<LogicalDependentJoin>();
		for (auto &corr : plan.correlated_columns) {
			auto entry = correlated_map.find(corr.binding);
			if (entry != correlated_map.end()) {
				corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			}
		}
	}
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (expr.depth <= lateral_depth) {
		// Indicates local correlations not relevant for the current the rewrite
		return nullptr;
	}
	// correlated column reference
	// replace with the entry referring to the duplicate eliminated scan
	// if this assertion occurs it generally means the bindings are inappropriate set in the binder or
	// we either missed to account for lateral binder or over-counted for the lateral binder
	D_ASSERT(expr.depth == 1 + lateral_depth);
	auto entry = correlated_map.find(expr.binding);
	D_ASSERT(entry != correlated_map.end());

	expr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
	if (recursive_rewrite) {
		D_ASSERT(expr.depth > 1);
		expr.depth--;
	} else {
		expr.depth = 0;
	}
	return nullptr;
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundSubqueryExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (!expr.IsCorrelated()) {
		return nullptr;
	}
	// subquery detected within this subquery
	// recursively rewrite it using the RewriteCorrelatedRecursive class
	RewriteCorrelatedRecursive rewrite(expr, base_binding, correlated_map);
	rewrite.RewriteCorrelatedSubquery(expr);
	return nullptr;
}

RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedRecursive(
    BoundSubqueryExpression &parent, ColumnBinding base_binding, column_binding_map_t<idx_t> &correlated_map)
    : parent(parent), base_binding(base_binding), correlated_map(correlated_map) {
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteJoinRefRecursive(BoundTableRef &ref) {
	// recursively rewrite bindings in the correlated columns for the table ref and all the children
	if (ref.type == TableReferenceType::JOIN) {
		auto &bound_join = ref.Cast<BoundJoinRef>();
		for (auto &corr : bound_join.correlated_columns) {
			auto entry = correlated_map.find(corr.binding);
			if (entry != correlated_map.end()) {
				corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			}
		}
		RewriteJoinRefRecursive(*bound_join.left);
		RewriteJoinRefRecursive(*bound_join.right);
	}
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedSubquery(
    BoundSubqueryExpression &expr) {
	// rewrite the binding in the correlated list of the subquery)
	for (auto &corr : expr.binder->correlated_columns) {
		auto entry = correlated_map.find(corr.binding);
		if (entry != correlated_map.end()) {
			corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
		}
	}
	// TODO: Cleanup and find a better way to do this
	auto &node = *expr.subquery;
	if (node.type == QueryNodeType::SELECT_NODE) {
		// Found an unplanned select node, need to update column bindings correlated columns in the from tables
		auto &bound_select = node.Cast<BoundSelectNode>();
		if (bound_select.from_table) {
			BoundTableRef &table_ref = *bound_select.from_table;
			RewriteJoinRefRecursive(table_ref);
		}
	}
	// now rewrite any correlated BoundColumnRef expressions inside the subquery
	ExpressionIterator::EnumerateQueryNodeChildren(*expr.subquery,
	                                               [&](Expression &child) { RewriteCorrelatedExpressions(child); });
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedExpressions(Expression &child) {
	if (child.type == ExpressionType::BOUND_COLUMN_REF) {
		// bound column reference
		auto &bound_colref = child.Cast<BoundColumnRefExpression>();
		if (bound_colref.depth == 0) {
			// not a correlated column, ignore
			return;
		}
		// correlated column
		// check the correlated map
		auto entry = correlated_map.find(bound_colref.binding);
		if (entry != correlated_map.end()) {
			// we found the column in the correlated map!
			// update the binding and reduce the depth by 1
			bound_colref.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			bound_colref.depth--;
		}
	} else if (child.type == ExpressionType::SUBQUERY) {
		// we encountered another subquery: rewrite recursively
		D_ASSERT(child.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &bound_subquery = child.Cast<BoundSubqueryExpression>();
		RewriteCorrelatedRecursive rewrite(bound_subquery, base_binding, correlated_map);
		rewrite.RewriteCorrelatedSubquery(bound_subquery);
	}
}

RewriteCountAggregates::RewriteCountAggregates(column_binding_map_t<idx_t> &replacement_map)
    : replacement_map(replacement_map) {
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
