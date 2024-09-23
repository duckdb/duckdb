#include "duckdb/planner/subquery/rewrite_subquery.hpp"

#include "duckdb/planner/operator/list.hpp"

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

RewriteSubquery::RewriteSubquery(const vector<idx_t> &table_index, idx_t lateral_depth,
                                 const vector<CorrelatedColumnInfo> &correlated_columns)
    : table_index(table_index), lateral_depth(lateral_depth), correlated_columns(correlated_columns) {
}

void RewriteSubquery::VisitOperator(duckdb::LogicalOperator &op) {
	if (table_index.empty()) {
		// no table index to rewrite: skip
		return;
	}

	// visit the children of the operator
	// check if op is a dependent join, if so, increment the lateral depth
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &join = op.Cast<LogicalDependentJoin>();
		VisitOperator(*op.children[0]);
		lateral_depth++;

		for (auto &col : correlated_columns) {
			for (auto &corr : join.correlated_columns) {
				if (corr.binding == col.binding) {
					continue;
				}
			}
			join.correlated_columns.emplace_back(col);
		}

		VisitOperator(*op.children[1]);
		lateral_depth--;
	} else {
		VisitOperatorChildren(op);
	}

	VisitOperatorExpressions(op);
}

unique_ptr<Expression> RewriteSubquery::VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// subquery detected within this subquery
	// recursively rewrite it using the RewriteCorrelatedRecursive class
	RewriteCorrelatedSubqueriesRecursive rewrite(table_index, lateral_depth, correlated_columns);
	rewrite.subquery_depth++;
	rewrite.RewriteCorrelatedSubquery(*expr.binder, *expr.subquery);
	add_correlated_columns = rewrite.add_correlated_columns;
	return nullptr;
}

RewriteCorrelatedSubqueriesRecursive::RewriteCorrelatedSubqueriesRecursive(
    const vector<idx_t> &table_index, idx_t lateral_depth, const vector<CorrelatedColumnInfo> &correlated_columns)
    : table_index(table_index), lateral_depth(lateral_depth), correlated_columns(correlated_columns) {
}

void RewriteCorrelatedSubqueriesRecursive::VisitBoundTableRef(BoundTableRef &ref) {
	if (ref.type == TableReferenceType::SUBQUERY) {
		auto &subquery = ref.Cast<BoundSubqueryRef>();
		this->binder = subquery.binder.get();
		RewriteCorrelatedSubquery(*subquery.binder, *subquery.subquery);
		return;
	} else if (ref.type == TableReferenceType::CTE) {
		auto &cteref = ref.Cast<BoundCTERef>();

		// check if this is the CTE we are looking for
		bool found = std::find(table_index.begin(), table_index.end(), cteref.cte_index) != table_index.end();

		if (found) {
			// this is the CTE we are looking for: add a filter to the CTE
			// we add a filter to the CTE that compares the correlated columns of the CTE to the correlated columns of
			// the outer query. This filter is added to the WHERE clause of the subquery.

			// first we need to find the correlated columns of the CTE

			Binder *current = this->binder;
			vector<CorrelatedColumnInfo> cte_correlated_columns;
			bool found = false;
			while (current) {

				auto rec_cte = current->recursive_ctes.find(cteref.cte_index);
				if (rec_cte != current->recursive_ctes.end()) {
					auto &rec_cte_info = rec_cte->second->Cast<LogicalCTE>();
					cte_correlated_columns = rec_cte_info.correlated_columns;
					found = true;
					break;
				}

				current = current->GetParentBinder().get();
			}

			D_ASSERT(found);

			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				auto &col = correlated_columns[i];
				auto col_copy = col;
				col_copy.depth += lateral_depth + subquery_depth;
				add_correlated_columns.push_back(col_copy);

				auto outer_binding = col.binding;
				auto outer = make_uniq<BoundColumnRefExpression>(col.name, col.type, outer_binding,
				                                                 col.depth + subquery_depth + lateral_depth);

				auto inner_binding = ColumnBinding(cteref.bind_index, cteref.bound_columns.size() + i);
				auto inner = make_uniq<BoundColumnRefExpression>(col.name, col.type, inner_binding);

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

void RewriteCorrelatedSubqueriesRecursive::RewriteCorrelatedSubquery(Binder &binder, BoundQueryNode &subquery) {
	this->binder = &binder;
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

	for (auto &col : add_correlated_columns) {
		bool skip = false;
		for (auto &corr : binder.correlated_columns) {
			if (corr.binding == col.binding) {
				skip = true;
				break;
			}
		}

		if (skip) {
			continue;
		}
		col.depth--;
		binder.AddCorrelatedColumn(col);
	}
}

void RewriteCorrelatedSubqueriesRecursive::VisitExpression(unique_ptr<Expression> &expression) {
	if (expression->type == ExpressionType::SUBQUERY) {
		// we encountered another subquery: rewrite recursively
		auto &bound_subquery = expression->Cast<BoundSubqueryExpression>();
		subquery_depth++;
		RewriteCorrelatedSubquery(*bound_subquery.binder, *bound_subquery.subquery);
	}
	// recurse into the children of this subquery
	BoundNodeVisitor::VisitExpression(expression);
}

} // namespace duckdb
