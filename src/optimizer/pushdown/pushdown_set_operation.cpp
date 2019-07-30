#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_subquery.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

static void ReplaceSetOpBindings(LogicalSetOperation &setop, Expression &expr, index_t child_index) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.binding.table_index == setop.table_index);
		assert(colref.binding.column_index < setop.column_count);
		assert(colref.depth == 0);
		// replace the reference to the set operation with a reference to the child subquery
		colref.binding.table_index = child_index;
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { ReplaceSetOpBindings(setop, child, child_index); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSetOperation(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::UNION || op->type == LogicalOperatorType::EXCEPT ||
	       op->type == LogicalOperatorType::INTERSECT);
	auto &setop = (LogicalSetOperation &)*op;

	assert(op->children.size() == 2);
	// create subqueries to wrap the children, if necessary
	for (index_t i = 0; i < 2; i++) {
		if (op->children[i]->type != LogicalOperatorType::SUBQUERY) {
			op->children[i] =
			    make_unique<LogicalSubquery>(move(op->children[i]), optimizer.binder.GenerateTableIndex());
		}
	}
	assert(op->children[0]->type == LogicalOperatorType::SUBQUERY &&
	       op->children[1]->type == LogicalOperatorType::SUBQUERY);
	index_t left_index = ((LogicalSubquery &)*op->children[0]).table_index;
	index_t right_index = ((LogicalSubquery &)*op->children[1]).table_index;

	// pushdown into set operation, we can duplicate the condition and pushdown the expressions into both sides
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	for (index_t i = 0; i < filters.size(); i++) {
		// first create a copy of the filter
		auto right_filter = make_unique<Filter>();
		right_filter->filter = filters[i]->filter->Copy();

		// in the original filter, rewrite references to the result of the union into references to the left_index
		ReplaceSetOpBindings(setop, *filters[i]->filter, left_index);
		// in the copied filter, rewrite references to the result of the union into references to the right_index
		ReplaceSetOpBindings(setop, *right_filter->filter, right_index);

		// extract bindings again
		filters[i]->ExtractBindings();
		right_filter->ExtractBindings();

		// move the filters into the child pushdown nodes
		left_pushdown.filters.push_back(move(filters[i]));
		right_pushdown.filters.push_back(move(right_filter));
	}

	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));
	return op;
}
