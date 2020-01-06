#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/table_binding_resolver.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

static void ReplaceSetOpBindings(vector<BoundTable> &bound_tables, Filter &filter, Expression &expr, LogicalSetOperation &setop) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.binding.table_index == setop.table_index);
		assert(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		index_t column_index = colref.binding.column_index;
		for (index_t i = 0; i < bound_tables.size(); i++) {
			auto &table = bound_tables[i];
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
	    expr, [&](Expression &child) { ReplaceSetOpBindings(bound_tables, filter, child, setop); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSetOperation(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::UNION || op->type == LogicalOperatorType::EXCEPT ||
	       op->type == LogicalOperatorType::INTERSECT);
	auto &setop = (LogicalSetOperation &)*op;

	assert(op->children.size() == 2);

	// find the bindings of the children of the set operation
	vector<BoundTable> bound_tables[2];
	for (index_t i = 0; i < 2; i++) {
		TableBindingResolver resolver;
		resolver.VisitOperator(*op->children[i]);
		bound_tables[i] = resolver.bound_tables;
	}

	// pushdown into set operation, we can duplicate the condition and pushdown the expressions into both sides
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	for (index_t i = 0; i < filters.size(); i++) {
		// first create a copy of the filter
		auto right_filter = make_unique<Filter>();
		right_filter->filter = filters[i]->filter->Copy();

		// in the original filter, rewrite references to the result of the union into references to the left_index
		ReplaceSetOpBindings(bound_tables[0], *filters[i], *filters[i]->filter, setop);
		// in the copied filter, rewrite references to the result of the union into references to the right_index
		ReplaceSetOpBindings(bound_tables[1], *right_filter, *right_filter->filter, setop);

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
