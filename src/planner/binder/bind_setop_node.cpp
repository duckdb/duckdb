#include "parser/expression/columnref_expression.hpp"
#include "parser/query_node/set_operation_node.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(SetOperationNode &statement) {
	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	assert(statement.left);
	assert(statement.right);

	Binder binder_left(context, this);
	Binder binder_right(context, this);

	binder_left.Bind(*statement.left);
	statement.setop_left_binder = move(binder_left.bind_context);

	binder_right.Bind(*statement.right);
	statement.setop_right_binder = move(binder_right.bind_context);

	// now handle the ORDER BY
	// get the selection list from one of the children, since a SetOp does not have its own selection list
	auto &select_list = statement.GetSelectList();
	for (auto &order : statement.orderby.orders) {
		VisitExpression(&order.expression);
		if (order.expression->type == ExpressionType::COLUMN_REF) {
			auto selection_ref = (ColumnRefExpression *)order.expression.get();
			if (selection_ref->column_name.empty()) {
				// this ORDER BY expression refers to a column in the select
				// clause by index e.g. ORDER BY 1 assign the type of the SELECT
				// clause
				if (selection_ref->index < 1 || selection_ref->index > select_list.size()) {
					throw BinderException("ORDER term out of range - should be between 1 and %d",
					                      (int)select_list.size());
				}
				selection_ref->return_type = select_list[selection_ref->index - 1]->return_type;
				selection_ref->reference = select_list[selection_ref->index - 1].get();
				selection_ref->index = selection_ref->index - 1;
			}
		}
		order.expression->ResolveType();
	}
}
