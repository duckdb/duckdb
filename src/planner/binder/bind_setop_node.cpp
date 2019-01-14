#include "parser/expression/bound_expression.hpp"
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
	BindOrderBy(statement.orderby, select_list, select_list.size());
}
