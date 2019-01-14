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

void ColumnBindingResolver::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::GET:
		Visit((LogicalGet &)op);
		break;
	case LogicalOperatorType::SUBQUERY:
		Visit((LogicalSubquery &)op);
		break;
	case LogicalOperatorType::TABLE_FUNCTION:
		Visit((LogicalTableFunction &)op);
		break;
	case LogicalOperatorType::JOIN:
		Visit((LogicalJoin &)op);
		break;
	case LogicalOperatorType::CROSS_PRODUCT:
		Visit((LogicalCrossProduct &)op);
		break;
	case LogicalOperatorType::UNION:
		Visit((LogicalUnion &)op);
		break;
	case LogicalOperatorType::EXCEPT:
		Visit((LogicalExcept &)op);
		break;
	case LogicalOperatorType::INTERSECT:
		Visit((LogicalIntersect &)op);
		break;
	case LogicalOperatorType::CREATE_INDEX:
		Visit((LogicalCreateIndex &)op);
		break;
	default:
		// for the operators we do not handle explicitly, we just visit the children
		LogicalOperatorVisitor::VisitOperator(op);
		break;
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

	LogicalOperatorVisitor::VisitOperatorExpressions(op);
}

void ColumnBindingResolver::BindTablesBinaryOp(LogicalOperator &op, bool append_right) {
	assert(op.children.size() == 2);
	// resolve the column indices of the left side
	VisitOperator(*op.children[0]);
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	VisitOperator(*op.children[1]);
	auto right_tables = bound_tables;

	// now merge the two together
	bound_tables = left_tables;
	if (append_right) {
		AppendTables(right_tables);
	}
}

void ColumnBindingResolver::Visit(LogicalCrossProduct &op) {
	BindTablesBinaryOp(op, true);
}

void ColumnBindingResolver::Visit(LogicalUnion &op) {
	BindTablesBinaryOp(op, false);
}

void ColumnBindingResolver::Visit(LogicalExcept &op) {
	BindTablesBinaryOp(op, false);
}

void ColumnBindingResolver::Visit(LogicalIntersect &op) {
	BindTablesBinaryOp(op, false);
}

void ColumnBindingResolver::Visit(LogicalGet &op) {
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
	resolver.VisitOperator(*op.children[0]);

	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.column_count;
	binding.column_offset =
	    bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	bound_tables.push_back(binding);
}

void ColumnBindingResolver::Visit(LogicalTableFunction &op) {
	LogicalOperatorVisitor::VisitOperatorExpressions(op);

	BoundTable binding;
	binding.table_index = op.table_index;
	binding.column_count = op.function->return_values.size();
	binding.column_offset =
	    bound_tables.size() == 0 ? 0 : bound_tables.back().column_offset + bound_tables.back().column_count;
	bound_tables.push_back(binding);
}

void ColumnBindingResolver::Visit(LogicalJoin &op) {
	// resolve the column indices of the left side
	VisitOperator(*op.children[0]);
	for (auto &cond : op.conditions) {
		VisitExpression(&cond.left);
	}
	// store the added tables
	auto left_tables = bound_tables;
	bound_tables.clear();

	// now resolve the column indices of the right side
	VisitOperator(*op.children[1]);
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
	assert(expr.op);
	// resolve the column ref indices of subqueries
	current_depth++;
	VisitOperator(*expr.op);
	current_depth--;
}
