#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundQueryNode> Binder::BindNode(RecursiveCTENode &statement) {
	auto result = make_unique<BoundRecursiveCTENode>();

	// first recursively visit the recursive CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	assert(statement.left);
	assert(statement.right);

	result->ctename = statement.ctename;
	result->union_all = statement.union_all;
	result->setop_index = GenerateTableIndex();

	result->left_binder = make_unique<Binder>(context, this);
	result->left = result->left_binder->BindNode(*statement.left);

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->left->names, result->left->types);

	result->right_binder = make_unique<Binder>(context, this);

	// Add bindings of left side to temporary CTE bindings context
	result->right_binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, result->left->names,
	                                                 result->left->types);
	result->right = result->right_binder->BindNode(*statement.right);

	// Check if there are aggregates present in the recursive term
	switch (result->right->type) {
	case QueryNodeType::SELECT_NODE:
		if (!((BoundSelectNode *)result->right.get())->aggregates.empty()) {
			throw Exception("Aggregate functions are not allowed in a recursive query's recursive term");
		}
		break;
	default:
		break;
	}

	result->names = result->left->names;

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	MoveCorrelatedExpressions(*result->right_binder);

	// now both sides have been bound we can resolve types
	if (result->left->types.size() != result->right->types.size()) {
		throw Exception("Set operations can only apply to expressions with the "
		                "same number of result columns");
	}

	// figure out the types of the recursive CTE result by picking the max of both
	for (idx_t i = 0; i < result->left->types.size(); i++) {
		auto result_type = MaxSQLType(result->left->types[i], result->right->types[i]);
		result->types.push_back(result_type);
	}
	if (statement.modifiers.size() > 0) {
		throw Exception("FIXME: bind modifiers in recursive CTE");
	}

	return move(result);
}
