#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"

namespace duckdb {

static void GatherAliases(BoundQueryNode &node, case_insensitive_map_t<idx_t> &aliases,
                          expression_map_t<idx_t> &expressions) {
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// setop, recurse
		auto &setop = (BoundSetOperationNode &)node;
		GatherAliases(*setop.left, aliases, expressions);
		GatherAliases(*setop.right, aliases, expressions);
	} else {
		// query node
		D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
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
					aliases[name] = DConstants::INVALID_INDEX;
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
					expressions[expr.get()] = DConstants::INVALID_INDEX;
				}
			} else {
				// not in there yet, just place it in there
				expressions[expr.get()] = i;
			}
		}
	}
}

static void PushProjection(BoundQueryNode &node, case_insensitive_map_t<idx_t> &node_names_map,
                           vector<string> &result_names, vector<LogicalType> &result_types) {

	bool need_reorder = false;
	idx_t result_size = result_names.size();

	vector<LogicalType> new_types(result_size);
	vector<string> new_names(result_size);
	vector<idx_t> reorder_index(result_size);
	unordered_set<idx_t> null_values_index_set;

	// Reorder names and types
	for (idx_t i = 0; i < result_size; ++i) {
		auto name_index = node_names_map.find(result_names[i]);
		bool name_exist = name_index != node_names_map.end();

		if (name_exist) {
			new_names[i] = std::move(node.names[name_index->second]);
			new_types[i] = std::move(node.types[name_index->second]);
			need_reorder = need_reorder || name_index->second != i;
			reorder_index[i] = name_index->second;
		} else {
			new_names[i] = result_names[i];
			new_types[i] = result_types[i];
			need_reorder = true;
			reorder_index[i] = i;
			null_values_index_set.insert(i);
		}
	}

	node.names = std::move(new_names);
	node.types = std::move(new_types);

	idx_t node_index;
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		auto &setop = (BoundSetOperationNode &)(node);
		node_index = setop.setop_index;
	} else {
		D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
		auto &select_node = (BoundSelectNode &)(node);
		node_index = select_node.projection_index;
	}

	if (need_reorder) {
		vector<unique_ptr<Expression>> reorder_expressions(result_size);

		if (node.type == QueryNodeType::SELECT_NODE) {
			auto &select_node = (BoundSelectNode &)(node);
			vector<unique_ptr<ParsedExpression>> original_expressions(result_size);
			for (idx_t i = 0; i < result_size; i++) {
				if (null_values_index_set.count(i) > 0) {
					reorder_expressions[i] = make_unique<BoundConstantExpression>(Value(result_types[i]));

					original_expressions[i] = make_unique<ConstantExpression>(Value(result_types[i]));
				} else {
					reorder_expressions[i] = make_unique<BoundColumnRefExpression>(
					    node.types[i], ColumnBinding(node_index, reorder_index[i]));

					original_expressions[i] = std::move(select_node.original_expressions[reorder_index[i]]);
				}
			}
			select_node.reorder_exprs = std::move(reorder_expressions);
			select_node.original_expressions = std::move(original_expressions);
		} else {
			D_ASSERT(node.type == QueryNodeType::SET_OPERATION_NODE);
			auto &setop = (BoundSetOperationNode &)(node);
			for (idx_t i = 0; i < result_size; i++) {
				if (null_values_index_set.count(i) > 0) {
					reorder_expressions[i] = make_unique<BoundConstantExpression>(Value(result_types[i]));
				} else {
					reorder_expressions[i] = make_unique<BoundColumnRefExpression>(
					    node.types[i], ColumnBinding(node_index, reorder_index[i]));
				}
			}
			setop.reorder_exprs = std::move(reorder_expressions);
		}
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SetOperationNode &statement) {
	auto result = make_unique<BoundSetOperationNode>();
	result->setop_type = statement.setop_type;
	result->reorder_index = GenerateTableIndex();

	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);

	result->setop_index = GenerateTableIndex();

	result->left_binder = Binder::CreateBinder(context, this);
	result->left_binder->can_contain_nulls = true;
	result->left = result->left_binder->BindNode(*statement.left);
	result->right_binder = Binder::CreateBinder(context, this);
	result->right_binder->can_contain_nulls = true;
	result->right = result->right_binder->BindNode(*statement.right);

	if (!statement.modifiers.empty()) {
		// handle the ORDER BY/DISTINCT clauses

		// we recursively visit the children of this node to extract aliases and expressions that can be referenced in
		// the ORDER BY
		case_insensitive_map_t<idx_t> alias_map;
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
	if (result->setop_type != SetOperationType::UNION_BY_NAME &&
	    result->left->types.size() != result->right->types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (result->setop_type == SetOperationType::UNION_BY_NAME) {
		case_insensitive_map_t<idx_t> left_names_map;
		case_insensitive_map_t<idx_t> right_names_map;

		BoundQueryNode *left_node = result->left.get();
		BoundQueryNode *right_node = result->right.get();

		for (idx_t i = 0; i < left_node->names.size(); ++i) {
			left_names_map[left_node->names[i]] = i;
		}

		for (idx_t i = 0; i < right_node->names.size(); ++i) {
			if (left_names_map.find(right_node->names[i]) == left_names_map.end()) {
				result->names.push_back(right_node->names[i]);
			}
			right_names_map[right_node->names[i]] = i;
		}

		idx_t new_size = result->names.size();

		for (idx_t i = 0; i < new_size; ++i) {
			auto left_index = left_names_map.find(result->names[i]);
			auto right_index = right_names_map.find(result->names[i]);
			bool left_exist = left_index != left_names_map.end();
			bool right_exist = right_index != right_names_map.end();
			LogicalType result_type;
			if (left_exist && right_exist) {
				result_type = LogicalType::MaxLogicalType(left_node->types[left_index->second],
				                                          right_node->types[right_index->second]);
			} else if (left_exist) {
				result_type = left_node->types[left_index->second];
			} else {
				D_ASSERT(right_exist);
				result_type = right_node->types[right_index->second];
			}

			if (!can_contain_nulls) {
				if (ExpressionBinder::ContainsNullType(result_type)) {
					result_type = ExpressionBinder::ExchangeNullType(result_type);
				}
			}
			result->types.push_back(result_type);
		}

		PushProjection(*result->left, left_names_map, result->names, result->types);
		PushProjection(*result->right, right_names_map, result->names, result->types);

	} else {
		// figure out the types of the setop result by picking the max of both
		for (idx_t i = 0; i < result->left->types.size(); i++) {
			auto result_type = LogicalType::MaxLogicalType(result->left->types[i], result->right->types[i]);
			if (!can_contain_nulls) {
				if (ExpressionBinder::ContainsNullType(result_type)) {
					result_type = ExpressionBinder::ExchangeNullType(result_type);
				}
			}
			result->types.push_back(result_type);
		}
	}

	// finally bind the types of the ORDER/DISTINCT clause expressions
	BindModifierTypes(*result, result->types, result->setop_index);
	return move(result);
}

} // namespace duckdb
