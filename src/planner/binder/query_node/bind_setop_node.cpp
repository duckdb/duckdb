#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"

using namespace duckdb;
using namespace std;

static void GatherAliases(QueryNode &node, unordered_map<string, idx_t> &aliases,
                          expression_map_t<idx_t> &expressions) {
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// setop, recurse
		auto &setop = (SetOperationNode &)node;
		GatherAliases(*setop.left, aliases, expressions);
		GatherAliases(*setop.right, aliases, expressions);
	} else {
		// query node
		assert(node.type == QueryNodeType::SELECT_NODE);
		auto &select = (SelectNode &)node;
		// fill the alias lists
		for (idx_t i = 0; i < select.select_list.size(); i++) {
			auto &expr = select.select_list[i];
			auto name = expr->GetName();
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

unique_ptr<BoundQueryNode> Binder::Bind(SetOperationNode &statement) {
	auto result = make_unique<BoundSetOperationNode>();
	result->setop_type = statement.setop_type;

	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	assert(statement.left);
	assert(statement.right);

	result->setop_index = GenerateTableIndex();

	vector<idx_t> order_references;
	if (statement.orders.size() > 0) {
		// handle the ORDER BY
		// NOTE: we handle the ORDER BY in SET OPERATIONS before binding the children
		// we do so we can perform expression comparisons BEFORE type resolution/binding

		// we recursively visit the children of this node to extract aliases and expressions that can be referenced in
		// the ORDER BY
		unordered_map<string, idx_t> alias_map;
		expression_map_t<idx_t> expression_map;
		GatherAliases(statement, alias_map, expression_map);
		// now we perform the actual resolution of the ORDER BY expressions
		for (idx_t i = 0; i < statement.orders.size(); i++) {
			auto &order = statement.orders[i].expression;
			if (order->type == ExpressionType::VALUE_CONSTANT) {
				// ORDER BY a constant
				auto &constant = (ConstantExpression &)*order;
				if (TypeIsIntegral(constant.value.type)) {
					// INTEGER constant: we use the integer as an index into the select list (e.g. ORDER BY 1)
					order_references.push_back(constant.value.GetValue<int64_t>() - 1);
					continue;
				}
			}
			if (order->type == ExpressionType::COLUMN_REF) {
				// ORDER BY column, check if it is an alias reference
				auto &colref = (ColumnRefExpression &)*order;
				if (colref.table_name.empty()) {
					auto entry = alias_map.find(colref.column_name);
					if (entry != alias_map.end()) {
						// found a matching entry
						if (entry->second == INVALID_INDEX) {
							// ambiguous reference
							throw BinderException("Ambiguous alias reference \"%s\"", colref.column_name.c_str());
						} else {
							order_references.push_back(entry->second);
							continue;
						}
					}
				}
			}
			// check if the ORDER BY clause matches any of the columns in the projection list of any of the children
			auto expr_ref = expression_map.find(order.get());
			if (expr_ref == expression_map.end()) {
				// not found
				throw BinderException("Could not ORDER BY column: add the expression/function to every SELECT, or move "
				                      "the UNION into a FROM clause.");
			}
			if (expr_ref->second == INVALID_INDEX) {
				throw BinderException("Ambiguous reference to column");
			}
			order_references.push_back(expr_ref->second);
		}
	}

	result->left_binder = make_unique<Binder>(context, this);
	result->left = result->left_binder->Bind(*statement.left);

	result->right_binder = make_unique<Binder>(context, this);
	result->right = result->right_binder->Bind(*statement.right);

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

	// if there are ORDER BY entries we create the BoundColumnRefExpressions
	assert(order_references.size() == statement.orders.size());
	for (idx_t i = 0; i < statement.orders.size(); i++) {
		auto entry = order_references[i];
		if (entry >= result->types.size()) {
			throw BinderException("ORDER term out of range - should be between 1 and %d", (int)result->types.size());
		}
		BoundOrderByNode node;

		node.expression = make_unique<BoundColumnRefExpression>(GetInternalType(result->types[entry]),
		                                                        ColumnBinding(result->setop_index, entry));
		node.type = statement.orders[i].type;
		result->orders.push_back(move(node));
	}
	return move(result);
}
