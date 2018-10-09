#include "execution/column_binding_resolver.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

static void PrintBoundTables(std::vector<BoundTable> &tables) {
	fprintf(stderr, "<<Bound Tables>>\n");
	for(auto &table : tables) {
		fprintf(stderr, "{Index: %zu, ColCount: %zu, Offset: %zu}\n", table.table_index, table.column_count, table.column_offset);
	}
}

void ColumnBindingResolver::AppendTables(
    std::vector<BoundTable> &right_tables) {
	size_t offset = bound_tables.size() == 0
	                    ? 0
	                    : bound_tables.back().column_offset +
	                          bound_tables.back().column_count;
	for (auto table : right_tables) {
		table.column_offset += offset;
		bound_tables.push_back(table);
	}
}

void ColumnBindingResolver::Visit(LogicalCrossProduct &op) {
	// resolve the column indices of the left side
	op.children[0]->Accept(this);
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	op.children[1]->Accept(this);
	auto right_tables = bound_tables;

	// now merge the two together
	bound_tables = left_tables;
	AppendTables(right_tables);
}

// FIXME: is this correct for the UNION?
void ColumnBindingResolver::Visit(LogicalUnion &op) {
	// resolve the column indices of the left side
	op.children[0]->Accept(this);
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	op.children[1]->Accept(this);
	auto right_tables = bound_tables;

	// now merge the two together
	bound_tables = left_tables;
	AppendTables(right_tables);
}

void ColumnBindingResolver::Visit(LogicalGet &op) {
	LogicalOperatorVisitor::Visit(op);
	if (!op.table) {
		return;
	}
	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.column_ids.size();
	binding.column_offset = bound_tables.size() == 0
	                            ? 0
	                            : bound_tables.back().column_offset +
	                                  bound_tables.back().column_count;
	bound_tables.push_back(binding);
}

void ColumnBindingResolver::Visit(LogicalJoin &op) {
	// resolve the column indices of the left side
	op.children[0]->Accept(this);
	for (auto &cond : op.conditions) {
		cond.left->Accept(this);
	}
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	op.children[1]->Accept(this);
	for (auto &cond : op.conditions) {
		cond.right->Accept(this);
	}
	auto right_tables = bound_tables;
	
	// now merge the two together
	bound_tables = left_tables;
	AppendTables(right_tables);
}

void ColumnBindingResolver::Visit(ColumnRefExpression &expr) {
	if (expr.index != (size_t)-1 || expr.reference ||
	    expr.depth != current_depth) {
		// not a base table reference OR should not be resolved by the current
		// resolver
		return;
	}
	for (auto &binding : bound_tables) {
		if (binding.table_index == expr.binding.table_index) {
			expr.index = binding.column_offset + expr.binding.column_index;
			break;
		}
	}
	assert(expr.index != (size_t)-1);
}

void ColumnBindingResolver::Visit(SubqueryExpression &expr) {
	// resolve the column ref indices of subqueries
	current_depth++;
	expr.op->Accept(this);
	current_depth--;
}
