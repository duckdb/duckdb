#include "duckdb/optimizer/column_lifetime_analyzer.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

void ColumnLifetimeAnalyzer::ExtractUnusedColumnBindings(const vector<ColumnBinding> &bindings,
                                                         column_binding_set_t &unused_bindings) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (column_references.find(bindings[i]) == column_references.end()) {
			unused_bindings.insert(bindings[i]);
		}
	}
}

void ColumnLifetimeAnalyzer::GenerateProjectionMap(vector<ColumnBinding> bindings,
                                                   column_binding_set_t &unused_bindings,
                                                   vector<idx_t> &projection_map) {
	D_ASSERT(projection_map.empty());
	if (unused_bindings.empty()) {
		return;
	}
	// now iterate over the result bindings of the child
	for (idx_t i = 0; i < bindings.size(); i++) {
		// if this binding does not belong to the unused bindings, add it to the projection map
		if (unused_bindings.find(bindings[i]) == unused_bindings.end()) {
			projection_map.push_back(i);
		}
	}
	if (projection_map.size() == bindings.size()) {
		projection_map.clear();
	}
}

void ColumnLifetimeAnalyzer::StandardVisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		// visit the duplicate eliminated columns on the LHS, if any
		auto &delim_join = op.Cast<LogicalComparisonJoin>();
		for (auto &expr : delim_join.duplicate_eliminated_columns) {
			VisitExpression(&expr);
		}
	}
	VisitOperatorChildren(op);
}

void ExtractColumnBindings(Expression &expr, vector<ColumnBinding> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_ref = expr.Cast<BoundColumnRefExpression>();
		bindings.push_back(bound_ref.binding);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractColumnBindings(child, bindings); });
}

void ColumnLifetimeAnalyzer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// FIXME: groups that are not referenced can be removed from projection
		// recurse into the children of the aggregate
		ColumnLifetimeAnalyzer analyzer(optimizer);
		analyzer.StandardVisitOperator(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &comp_join = op.Cast<LogicalComparisonJoin>();
		comp_join.left_projection_map.clear();
		comp_join.right_projection_map.clear();
		if (everything_referenced) {
			break;
		}

		// FIXME for now, we only push into the projection map for equality (hash) joins
		idx_t has_range = 0;
		if (!comp_join.HasEquality(has_range) || optimizer.context.config.prefer_range_joins) {
			return;
		}

		column_binding_set_t lhs_unused;
		column_binding_set_t rhs_unused;
		ExtractUnusedColumnBindings(op.children[0]->GetColumnBindings(), lhs_unused);
		ExtractUnusedColumnBindings(op.children[1]->GetColumnBindings(), rhs_unused);

		StandardVisitOperator(op);

		// then generate the projection map
		if (op.type != LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			// FIXME: left_projection_map in ASOF join
			GenerateProjectionMap(op.children[0]->GetColumnBindings(), lhs_unused, comp_join.left_projection_map);
		}
		GenerateProjectionMap(op.children[1]->GetColumnBindings(), rhs_unused, comp_join.right_projection_map);
		return;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		// for set operations/materialized CTEs we don't remove anything, just recursively visit the children
		// FIXME: for UNION we can remove unreferenced columns as long as everything_referenced is false (i.e. we
		// encounter a UNION node that is not preceded by a DISTINCT)
		ColumnLifetimeAnalyzer analyzer(optimizer, true);
		analyzer.StandardVisitOperator(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// then recurse into the children of this projection
		ColumnLifetimeAnalyzer analyzer(optimizer);
		analyzer.StandardVisitOperator(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		order.projection_map.clear();
		if (everything_referenced) {
			break;
		}

		column_binding_set_t unused_bindings;
		ExtractUnusedColumnBindings(op.children[0]->GetColumnBindings(), unused_bindings);

		StandardVisitOperator(op);

		GenerateProjectionMap(op.children[0]->GetColumnBindings(), unused_bindings, order.projection_map);
		return;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		// distinct, all projected columns are used for the DISTINCT computation
		// mark all columns as used and continue to the children
		// FIXME: DISTINCT with expression list does not implicitly reference everything
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		filter.projection_map.clear();
		if (everything_referenced) {
			break;
		}

		// filter, figure out which columns are not needed after the filter
		column_binding_set_t unused_bindings;
		ExtractUnusedColumnBindings(op.children[0]->GetColumnBindings(), unused_bindings);

		StandardVisitOperator(op);

		// then generate the projection map
		GenerateProjectionMap(op.children[0]->GetColumnBindings(), unused_bindings, filter.projection_map);

		return;
	}
	default:
		break;
	}
	StandardVisitOperator(op);
}

unique_ptr<Expression> ColumnLifetimeAnalyzer::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	column_references.insert(expr.binding);
	return nullptr;
}

unique_ptr<Expression> ColumnLifetimeAnalyzer::VisitReplace(BoundReferenceExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	// BoundReferenceExpression should not be used here yet, they only belong in the physical plan
	throw InternalException("BoundReferenceExpression should not be used here yet!");
}

} // namespace duckdb
