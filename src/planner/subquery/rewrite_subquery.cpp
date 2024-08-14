#include "duckdb/planner/subquery/rewrite_subquery.hpp"

#include "duckdb/planner/operator/list.hpp"

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

RewriteSubquery::RewriteSubquery(const vector<idx_t> &table_index, idx_t lateral_depth, ColumnBinding base_binding,
                                 const vector<CorrelatedColumnInfo> &correlated_columns)
    : table_index(table_index), lateral_depth(lateral_depth), base_binding(base_binding),
      correlated_columns(correlated_columns) {
}

void RewriteSubquery::VisitOperator(duckdb::LogicalOperator &op) {
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> RewriteSubquery::VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// subquery detected within this subquery
	// recursively rewrite it using the RewriteCorrelatedRecursive class
	RewriteCorrelatedSubqueriesRecursive rewrite(table_index, lateral_depth, this->base_binding, correlated_columns);
	bool rewrite_cte = expr.subquery_type == SubqueryType::EXISTS || expr.subquery_type == SubqueryType::NOT_EXISTS;
	rewrite.RewriteCorrelatedSubquery(*expr.binder, *expr.subquery, rewrite_cte);
	return nullptr;
}

RewriteCorrelatedSubqueriesRecursive::RewriteCorrelatedSubqueriesRecursive(
    const vector<idx_t> &table_index, idx_t lateral_depth, ColumnBinding base_binding,
    const vector<CorrelatedColumnInfo> &correlated_columns)
    : table_index(table_index), lateral_depth(lateral_depth), base_binding(base_binding),
      correlated_columns(correlated_columns) {
}

void RewriteCorrelatedSubqueriesRecursive::VisitBoundTableRef(BoundTableRef &ref) {
	if (ref.type == TableReferenceType::SUBQUERY) {
		auto &subquery = ref.Cast<BoundSubqueryRef>();
		RewriteCorrelatedSubquery(*subquery.binder, *subquery.subquery, add_filter);
		return;
	} else if (ref.type == TableReferenceType::CTE && add_filter) {
		auto &cteref = ref.Cast<BoundCTERef>();

		// check if this is the CTE we are looking for
		bool found = std::find(table_index.begin(), table_index.end(), cteref.cte_index) != table_index.end();

		if (found) {
			// this is the CTE we are looking for: add a filter to the CTE
			// we add a filter to the CTE that compares the correlated columns of the CTE to the correlated columns of
			// the outer query. This filter is added to the WHERE clause of the subquery.
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				auto &col = correlated_columns[i];
				auto outer = make_uniq<BoundColumnRefExpression>(
				    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i),
				    lateral_depth + 1);

				auto inner = make_uniq<BoundColumnRefExpression>(
				    col.name, col.type, ColumnBinding(cteref.bind_index, cteref.bound_columns.size() + i));
				auto comp = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
				                                                 std::move(outer), std::move(inner));
				if (condition) {
					auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(comp),
					                                                  std::move(condition));
					condition = std::move(conj);
				} else {
					condition = std::move(comp);
				}
			}
		}
	}
	// visit the children of the table ref
	BoundNodeVisitor::VisitBoundTableRef(ref);
}

void RewriteCorrelatedSubqueriesRecursive::RewriteCorrelatedSubquery(Binder &binder, BoundQueryNode &subquery,
                                                                     bool add_filter) {
	this->add_filter = add_filter;

	VisitBoundQueryNode(subquery);

	if (subquery.type == QueryNodeType::SELECT_NODE && condition) {
		auto &query = subquery.Cast<BoundSelectNode>();
		if (query.where_clause) {
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
			                                                  std::move(query.where_clause), std::move(condition));
			query.where_clause = std::move(conj);
		} else {
			query.where_clause = std::move(condition);
		}
	}
}

void RewriteCorrelatedSubqueriesRecursive::VisitExpression(unique_ptr<Expression> &expression) {
	if (expression->type == ExpressionType::SUBQUERY) {
		// we encountered another subquery: rewrite recursively
		auto &bound_subquery = expression->Cast<BoundSubqueryExpression>();
		bool rewrite_cte = bound_subquery.subquery_type == SubqueryType::EXISTS ||
		                   bound_subquery.subquery_type == SubqueryType::NOT_EXISTS;
		if (rewrite_cte) {
			lateral_depth++;
		}
		RewriteCorrelatedSubquery(*bound_subquery.binder, *bound_subquery.subquery, rewrite_cte);

		if (rewrite_cte) {
			lateral_depth--;
		}
	}
	// recurse into the children of this subquery
	BoundNodeVisitor::VisitExpression(expression);
}

} // namespace duckdb
