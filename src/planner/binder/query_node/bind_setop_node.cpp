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
                          parsed_expression_map_t<idx_t> &expressions, const vector<idx_t> &reorder_idx) {
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// setop, recurse
		auto &setop = node.Cast<BoundSetOperationNode>();

		// create new reorder index
		if (setop.setop_type == SetOperationType::UNION_BY_NAME) {
			vector<idx_t> new_left_reorder_idx(setop.left_reorder_idx.size());
			vector<idx_t> new_right_reorder_idx(setop.right_reorder_idx.size());
			for (idx_t i = 0; i < setop.left_reorder_idx.size(); ++i) {
				new_left_reorder_idx[i] = reorder_idx[setop.left_reorder_idx[i]];
			}

			for (idx_t i = 0; i < setop.right_reorder_idx.size(); ++i) {
				new_right_reorder_idx[i] = reorder_idx[setop.right_reorder_idx[i]];
			}

			// use new reorder index
			GatherAliases(*setop.left, aliases, expressions, new_left_reorder_idx);
			GatherAliases(*setop.right, aliases, expressions, new_right_reorder_idx);
			return;
		}

		GatherAliases(*setop.left, aliases, expressions, reorder_idx);
		GatherAliases(*setop.right, aliases, expressions, reorder_idx);
	} else {
		// query node
		D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
		auto &select = node.Cast<BoundSelectNode>();
		// fill the alias lists
		for (idx_t i = 0; i < select.names.size(); i++) {
			auto &name = select.names[i];
			auto &expr = select.original_expressions[i];
			// first check if the alias is already in there
			auto entry = aliases.find(name);

			idx_t index = reorder_idx[i];

			if (entry == aliases.end()) {
				// the alias is not in there yet, just assign it
				aliases[name] = index;
			}
			// now check if the node is already in the set of expressions
			auto expr_entry = expressions.find(*expr);
			if (expr_entry != expressions.end()) {
				// the node is in there
				// repeat the same as with the alias: if there is an ambiguity we insert "-1"
				if (expr_entry->second != index) {
					expressions[*expr] = DConstants::INVALID_INDEX;
				}
			} else {
				// not in there yet, just place it in there
				expressions[*expr] = index;
			}
		}
	}
}

static void BuildUnionByNameInfo(BoundSetOperationNode &result, bool can_contain_nulls) {
	D_ASSERT(result.setop_type == SetOperationType::UNION_BY_NAME);
	case_insensitive_map_t<idx_t> left_names_map;
	case_insensitive_map_t<idx_t> right_names_map;

	BoundQueryNode *left_node = result.left.get();
	BoundQueryNode *right_node = result.right.get();

	// Build a name_map to use to check if a name exists
	// We throw a binder exception if two same name in the SELECT list
	for (idx_t i = 0; i < left_node->names.size(); ++i) {
		if (left_names_map.find(left_node->names[i]) != left_names_map.end()) {
			throw BinderException("UNION(ALL) BY NAME operation doesn't support same name in SELECT list");
		}
		left_names_map[left_node->names[i]] = i;
	}

	for (idx_t i = 0; i < right_node->names.size(); ++i) {
		if (right_names_map.find(right_node->names[i]) != right_names_map.end()) {
			throw BinderException("UNION(ALL) BY NAME operation doesn't support same name in SELECT list");
		}
		if (left_names_map.find(right_node->names[i]) == left_names_map.end()) {
			result.names.push_back(right_node->names[i]);
		}
		right_names_map[right_node->names[i]] = i;
	}

	idx_t new_size = result.names.size();
	bool need_reorder = false;
	vector<idx_t> left_reorder_idx(left_node->names.size());
	vector<idx_t> right_reorder_idx(right_node->names.size());

	// Construct return type and reorder_idxs
	// reorder_idxs is used to gather correct alias_map
	// and expression_map in GatherAlias(...)
	for (idx_t i = 0; i < new_size; ++i) {
		auto left_index = left_names_map.find(result.names[i]);
		auto right_index = right_names_map.find(result.names[i]);
		bool left_exist = left_index != left_names_map.end();
		bool right_exist = right_index != right_names_map.end();
		LogicalType result_type;
		if (left_exist && right_exist) {
			result_type = LogicalType::MaxLogicalType(left_node->types[left_index->second],
			                                          right_node->types[right_index->second]);
			if (left_index->second != i || right_index->second != i) {
				need_reorder = true;
			}
			left_reorder_idx[left_index->second] = i;
			right_reorder_idx[right_index->second] = i;
		} else if (left_exist) {
			result_type = left_node->types[left_index->second];
			need_reorder = true;
			left_reorder_idx[left_index->second] = i;
		} else {
			D_ASSERT(right_exist);
			result_type = right_node->types[right_index->second];
			need_reorder = true;
			right_reorder_idx[right_index->second] = i;
		}

		if (!can_contain_nulls) {
			if (ExpressionBinder::ContainsNullType(result_type)) {
				result_type = ExpressionBinder::ExchangeNullType(result_type);
			}
		}

		result.types.push_back(result_type);
	}

	result.left_reorder_idx = std::move(left_reorder_idx);
	result.right_reorder_idx = std::move(right_reorder_idx);

	// If reorder is required, collect reorder expressions for push projection
	// into the two child nodes of union node
	if (need_reorder) {
		for (idx_t i = 0; i < new_size; ++i) {
			auto left_index = left_names_map.find(result.names[i]);
			auto right_index = right_names_map.find(result.names[i]);
			bool left_exist = left_index != left_names_map.end();
			bool right_exist = right_index != right_names_map.end();
			unique_ptr<Expression> left_reorder_expr;
			unique_ptr<Expression> right_reorder_expr;
			if (left_exist && right_exist) {
				left_reorder_expr = make_uniq<BoundColumnRefExpression>(
				    left_node->types[left_index->second], ColumnBinding(left_node->GetRootIndex(), left_index->second));
				right_reorder_expr =
				    make_uniq<BoundColumnRefExpression>(right_node->types[right_index->second],
				                                        ColumnBinding(right_node->GetRootIndex(), right_index->second));
			} else if (left_exist) {
				left_reorder_expr = make_uniq<BoundColumnRefExpression>(
				    left_node->types[left_index->second], ColumnBinding(left_node->GetRootIndex(), left_index->second));
				// create null value here
				right_reorder_expr = make_uniq<BoundConstantExpression>(Value(result.types[i]));
			} else {
				D_ASSERT(right_exist);
				left_reorder_expr = make_uniq<BoundConstantExpression>(Value(result.types[i]));
				right_reorder_expr =
				    make_uniq<BoundColumnRefExpression>(right_node->types[right_index->second],
				                                        ColumnBinding(right_node->GetRootIndex(), right_index->second));
			}
			result.left_reorder_exprs.push_back(std::move(left_reorder_expr));
			result.right_reorder_exprs.push_back(std::move(right_reorder_expr));
		}
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SetOperationNode &statement) {
	auto result = make_uniq<BoundSetOperationNode>();
	result->setop_type = statement.setop_type;
	result->setop_all = statement.setop_all;

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
		BuildUnionByNameInfo(*result, can_contain_nulls);

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

	if (!statement.modifiers.empty()) {
		// handle the ORDER BY/DISTINCT clauses

		// we recursively visit the children of this node to extract aliases and expressions that can be referenced
		// in the ORDER BY
		case_insensitive_map_t<idx_t> alias_map;
		parsed_expression_map_t<idx_t> expression_map;

		if (result->setop_type == SetOperationType::UNION_BY_NAME) {
			GatherAliases(*result->left, alias_map, expression_map, result->left_reorder_idx);
			GatherAliases(*result->right, alias_map, expression_map, result->right_reorder_idx);
		} else {
			vector<idx_t> reorder_idx;
			for (idx_t i = 0; i < result->names.size(); i++) {
				reorder_idx.push_back(i);
			}
			GatherAliases(*result, alias_map, expression_map, reorder_idx);
		}
		// now we perform the actual resolution of the ORDER BY/DISTINCT expressions
		OrderBinder order_binder({result->left_binder.get(), result->right_binder.get()}, result->setop_index,
		                         alias_map, expression_map, result->names.size());
		BindModifiers(order_binder, statement, *result);
	}

	// finally bind the types of the ORDER/DISTINCT clause expressions
	BindModifierTypes(*result, result->types, result->setop_index);
	return std::move(result);
}

} // namespace duckdb
