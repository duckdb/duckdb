#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"

using namespace std;

namespace duckdb {

static void GatherAliases(BoundQueryNode &node, unordered_map<string, idx_t> &aliases,
                          expression_map_t<idx_t> &expressions) {
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// setop, recurse
		auto &setop = (BoundSetOperationNode &)node;
		GatherAliases(*setop.left, aliases, expressions);
		GatherAliases(*setop.right, aliases, expressions);
	} else {
		// query node
		assert(node.type == QueryNodeType::SELECT_NODE);
		auto &select = (BoundSelectNode &)node;
		// fill the alias lists
		for (idx_t i = 0; i < select.names.size(); i++) {
			auto &name = select.names[i];
			auto &expr = select.original_expressions[i];
			// first check if the alias is already in there
			auto entry = aliases.find(name);
			if (entry != aliases.end()) {
				// the alias already exists
				// check if there is a conflict
				if (entry->second != i) {
					// there is a conflict
					// we place "-1" in the aliases map at this location
					// "-1" signifies that there is an ambiguous reference
					aliases[name] = INVALID_INDEX;
				}
			} else {
				// the alias is not in there yet, just assign it
				aliases[name] = i;
			}
			// now check if the node is already in the set of expressions
			auto expr_entry = expressions.find(expr.get());
			if (expr_entry != expressions.end()) {
				// the node is in there
				// repeat the same as with the alias: if there is an ambiguity we insert "-1"
				if (expr_entry->second != i) {
					expressions[expr.get()] = INVALID_INDEX;
				}
			} else {
				// not in there yet, just place it in there
				expressions[expr.get()] = i;
			}
		}
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SetOperationNode &statement) {
	auto result = make_unique<BoundSetOperationNode>();
	result->setop_type = statement.setop_type;

	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	assert(statement.left);
	assert(statement.right);

	result->setop_index = GenerateTableIndex();

	result->left_binder = make_unique<Binder>(context, this);
	result->left = result->left_binder->BindNode(*statement.left);

	result->right_binder = make_unique<Binder>(context, this);
	result->right = result->right_binder->BindNode(*statement.right);

	if (statement.modifiers.size() > 0) {
		// handle the ORDER BY/DISTINCT clauses

		// we recursively visit the children of this node to extract aliases and expressions that can be referenced in
		// the ORDER BY
		unordered_map<string, idx_t> alias_map;
		expression_map_t<idx_t> expression_map;
		GatherAliases(*result, alias_map, expression_map);

		// now we perform the actual resolution of the ORDER BY/DISTINCT expressions
		OrderBinder order_binder({result->left_binder.get(), result->right_binder.get()}, result->setop_index,
		                         alias_map, expression_map, statement.left->GetSelectList().size());
		BindModifiers(order_binder, statement, *result);
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

	// figure out the types of the setop result by picking the max of both
	for (idx_t i = 0; i < result->left->types.size(); i++) {
		auto result_type = MaxSQLType(result->left->types[i], result->right->types[i]);
		result->types.push_back(result_type);
	}

	// finally bind the types of the ORDER/DISTINCT clause expressions
	BindModifierTypes(*result, result->types, result->setop_index);
	return move(result);
}

} // namespace duckdb
