#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

using namespace duckdb;
using namespace std;

BindResult SelectBinder::BindUnnest(FunctionExpression &function, idx_t depth) {
	// bind the children of the function expression
	string error;
	if (function.children.size() != 1) {
		return BindResult("Unnest() needs exactly one child expressions");
	}
	BindChild(function.children[0], depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	auto &child = (BoundExpression &)*function.children[0];
	if (child.sql_type.id != SQLTypeId::LIST) {
		return BindResult("Unnest() can only be applied to lists");
	}
	SQLType return_type = SQLType::ANY;
	assert(child.sql_type.child_type.size() <= 1);
	if (child.sql_type.child_type.size() == 1) {
		return_type = child.sql_type.child_type[0].second;
	}

	auto result = make_unique<BoundUnnestExpression>(return_type);
	result->child = move(child.expr);

	auto unnest_index = node.unnests.size();
	node.unnests.push_back(move(result));

	// TODO what if we have multiple unnests in the same projection list? ignore for now

	// now create a column reference referring to the aggregate
	auto colref = make_unique<BoundColumnRefExpression>(
	    function.alias.empty() ? node.unnests[unnest_index]->ToString() : function.alias,
	    node.unnests[unnest_index]->return_type, ColumnBinding(node.unnest_index, unnest_index), depth);
	// move the aggregate expression into the set of bound aggregates
	return BindResult(move(colref), return_type);
}
