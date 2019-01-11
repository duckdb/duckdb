#include "execution/column_binding_resolver.hpp"

#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void ColumnBindingResolver::AppendTables(vector<BoundTable> &right_tables) {
	size_t offset = bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	for (auto table : right_tables) {
		table.column_offset += offset;
		bound_tables.push_back(table);
	}
}

void ColumnBindingResolver::Visit(LogicalCreateIndex &op) {
	// add the table to the column binding resolver
	// since we only have one table in the CREATE INDEX statement there is no
	// offset
	BoundTable binding;
	binding.table_index = 0;
	binding.column_count = op.table.columns.size();
	binding.column_offset = 0;
	bound_tables.push_back(binding);
	LogicalOperatorVisitor::Visit(op);
}

static void BindTablesBinaryOp(LogicalOperator &op, ColumnBindingResolver *res, bool append_right) {
	assert(res);

	// resolve the column indices of the left side
	op.children[0]->Accept(res);
	// store the added tables
	auto left_tables = res->bound_tables;
	res->bound_tables.clear();

	// now resolve the column indices of the right side
	op.children[1]->Accept(res);
	auto right_tables = res->bound_tables;

	// now merge the two together
	res->bound_tables = left_tables;
	if (append_right) {
		res->AppendTables(right_tables);
	}
}

void ColumnBindingResolver::Visit(LogicalCrossProduct &op) {
	BindTablesBinaryOp(op, this, true);
}

void ColumnBindingResolver::Visit(LogicalUnion &op) {
	BindTablesBinaryOp(op, this, false);
}

void ColumnBindingResolver::Visit(LogicalExcept &op) {
	BindTablesBinaryOp(op, this, false);
}

void ColumnBindingResolver::Visit(LogicalIntersect &op) {
	BindTablesBinaryOp(op, this, false);
}

void ColumnBindingResolver::Visit(LogicalGet &op) {
	LogicalOperatorVisitor::Visit(op);
	if (!op.table) {
		return;
	}
	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.column_ids.size();
	binding.column_offset =
	    bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	bound_tables.push_back(binding);
}

void ColumnBindingResolver::Visit(LogicalSubquery &op) {
	// we resolve the subquery separately
	ColumnBindingResolver resolver;
	op.AcceptChildren(&resolver);

	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.column_count;
	binding.column_offset =
	    bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	bound_tables.push_back(binding);
}

void ColumnBindingResolver::Visit(LogicalTableFunction &op) {
	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.function->return_values.size();
	binding.column_offset =
	    bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	bound_tables.push_back(binding);
}

void ColumnBindingResolver::Visit(LogicalJoin &op) {
	// resolve the column indices of the left side
	op.children[0]->Accept(this);
	for (auto &cond : op.conditions) {
		VisitExpression(&cond.left);
	}
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	op.children[1]->Accept(this);
	for (auto &cond : op.conditions) {
		VisitExpression(&cond.right);
	}
	auto right_tables = bound_tables;

	if (op.type != JoinType::ANTI && op.type != JoinType::SEMI) {
		// for normal joins the two results are combined
		bound_tables = left_tables;
		AppendTables(right_tables);
	} else {
		// for semi/anti joins the result is just the left side
		bound_tables = left_tables;
	}
}

void ColumnBindingResolver::Visit(ColumnRefExpression &expr) {
	if (expr.index != (size_t)-1 || expr.reference || expr.depth != current_depth) {
		// not a base table reference OR should not be resolved by the current
		// resolver
		return;
	}
	for (auto &binding : bound_tables) {
		if (binding.table_index == expr.binding.table_index) {
			expr.index = binding.column_offset + expr.binding.column_index;
			assert(expr.binding.column_index < binding.column_count);
			break;
		}
	}
	if (expr.index == (size_t)-1) {
		throw Exception("Failed to bind column ref");
	}
}

void ColumnBindingResolver::Visit(SubqueryExpression &expr) {
	// resolve the column ref indices of subqueries
	current_depth++;
	expr.op->Accept(this);
	current_depth--;
}
