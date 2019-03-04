#include "parser/expression/bound_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/query_node/set_operation_node.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

static void GatherAliases(QueryNode &node, unordered_map<string, uint32_t> &aliases,
                          expression_map_t<uint32_t> &expressions) {
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
		for (size_t i = 0; i < select.select_list.size(); i++) {
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
					aliases[name] = (uint32_t)-1;
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
					expressions[expr.get()] = (uint32_t)-1;
				}
			} else {
				// not in there yet, just place it in there
				expressions[expr.get()] = i;
			}
		}
	}
}

void Binder::Bind(SetOperationNode &statement) {
	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	assert(statement.left);
	assert(statement.right);

	auto &binding = statement.binding;
	binding.setop_index = GenerateTableIndex();

	vector<size_t> order_references;
	if (statement.HasOrder()) {
		// handle the ORDER BY
		// NOTE: we handle the ORDER BY in SET OPERATIONS before binding the children
		// we do so we can perform expression comparisons BEFORE type resolution
		// e.g. if the ordering is ORDER BY a + 10, the return type of all expressions will be "TypeId::INVALID" which
		// will allow for equality comparisons

		// we recursively visit the children of this node to extract aliases and expressions that can be referenced in
		// the ORDER BY
		unordered_map<string, uint32_t> alias_map;
		expression_map_t<uint32_t> expression_map;
		GatherAliases(statement, alias_map, expression_map);
		// now we perform the actual resolution of the ORDER BY expressions
		for (size_t i = 0; i < statement.orderby.orders.size(); i++) {
			auto &order = statement.orderby.orders[i].expression;
			if (order->type == ExpressionType::VALUE_CONSTANT) {
				// ORDER BY a constant
				auto &constant = (ConstantExpression &)*order;
				if (TypeIsIntegral(constant.value.type)) {
					// INTEGER constant: we use the integer as an index into the select list (e.g. ORDER BY 1)
					order_references.push_back(constant.value.GetNumericValue() - 1);
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
						if (entry->second == (uint32_t)-1) {
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
			if (expr_ref->second == (uint32_t)-1) {
				throw BinderException("Ambiguous reference to column");
			}
			order_references.push_back(expr_ref->second);
		}
	}



	binding.left_binder = make_unique<Binder>(context, this);
	binding.left_binder->Bind(*statement.left);

	binding.right_binder = make_unique<Binder>(context, this);
	binding.right_binder->Bind(*statement.right);

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*binding.left_binder);
	MoveCorrelatedExpressions(*binding.right_binder);

	// now both sides have been bound we can resolve types
	if (statement.left->types.size() != statement.right->types.size()) {
		throw Exception("Set operations can only apply to expressions with the "
		                "same number of result columns");
	}

	// figure out the types of the setop result by picking the max of both
	for (size_t i = 0; i < statement.left->types.size(); i++) {
		auto result_type = std::max(statement.left->types[i], statement.right->types[i]);
		statement.types.push_back(result_type);
	}

	// if there are ORDER BY entries we create the BoundExpressions
	assert(order_references.size() == statement.orderby.orders.size());
	for (size_t i = 0; i < statement.orderby.orders.size(); i++) {
		auto entry = order_references[i];
		if (entry >= statement.types.size()) {
			throw BinderException("ORDER term out of range - should be between 1 and %d", (int)statement.types.size());
		}
		statement.orderby.orders[i].expression = make_unique<BoundColumnRefExpression>("", statement.types[entry], ColumnBinding(binding.setop_index, entry));
	}
}
