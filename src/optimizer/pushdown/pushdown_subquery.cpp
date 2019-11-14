#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_subquery.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

static void RewriteSubqueryExpressionBindings(Filter &filter, Expression &expr, LogicalSubquery &subquery) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.binding.table_index == subquery.table_index);
		assert(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		index_t column_index = colref.binding.column_index;
		for (index_t i = 0; i < subquery.bound_tables.size(); i++) {
			auto &table = subquery.bound_tables[i];
			if (column_index < table.column_count) {
				// the binding belongs to this table, update the column binding
				colref.binding.table_index = table.table_index;
				colref.binding.column_index = column_index;
				filter.bindings.insert(table.table_index);
				return;
			}
			column_index -= table.column_count;
		}
		// table could not be found!
		assert(0);
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { RewriteSubqueryExpressionBindings(filter, child, subquery); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSubquery(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::SUBQUERY);
	auto &subquery = (LogicalSubquery &)*op;
	// push filter through logical subquery
	// all the BoundColumnRefExpressions in the filter should refer to the LogicalSubquery
	// we need to rewrite them to refer to the underlying bound tables instead
	for (index_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		assert(f.bindings.size() <= 1);
		f.bindings.clear();
		// rewrite the bindings within this subquery
		RewriteSubqueryExpressionBindings(f, *f.filter, subquery);
	}
	// now continue the pushdown into the child
	subquery.children[0] = Rewrite(move(subquery.children[0]));
	return op;
}
