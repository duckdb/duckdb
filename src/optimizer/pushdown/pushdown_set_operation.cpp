#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

static void ReplaceSetOpBindings(vector<ColumnBinding> &bindings, Filter &filter, Expression &expr,
                                 LogicalSetOperation &setop) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.binding.table_index == setop.table_index);
		assert(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		colref.binding = bindings[colref.binding.column_index];
		filter.bindings.insert(colref.binding.table_index);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ReplaceSetOpBindings(bindings, filter, child, setop); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSetOperation(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::UNION || op->type == LogicalOperatorType::EXCEPT ||
	       op->type == LogicalOperatorType::INTERSECT);
	auto &setop = (LogicalSetOperation &)*op;

	assert(op->children.size() == 2);
	auto left_bindings = op->children[0]->GetColumnBindings();
	auto right_bindings = op->children[1]->GetColumnBindings();

	// pushdown into set operation, we can duplicate the condition and pushdown the expressions into both sides
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	for (idx_t i = 0; i < filters.size(); i++) {
		// first create a copy of the filter
		auto right_filter = make_unique<Filter>();
		right_filter->filter = filters[i]->filter->Copy();

		// in the original filter, rewrite references to the result of the union into references to the left_index
		ReplaceSetOpBindings(left_bindings, *filters[i], *filters[i]->filter, setop);
		// in the copied filter, rewrite references to the result of the union into references to the right_index
		ReplaceSetOpBindings(right_bindings, *right_filter, *right_filter->filter, setop);

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
