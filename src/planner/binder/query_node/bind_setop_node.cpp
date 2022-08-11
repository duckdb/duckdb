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

void Binder::PushProjection(BoundQueryNode &node, case_insensitive_map_t<idx_t> &node_names_map,
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

	idx_t prune_index;
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		auto &setop = (BoundSetOperationNode &)(node);
		prune_index = setop.reorder_index;
	} else {
		D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
		auto &select_node = (BoundSelectNode &)(node);
		prune_index = select_node.prune_index;
	}

	if (need_reorder) {
		vector<unique_ptr<Expression>> reorder_expressions(result_names.size());
		for (idx_t i = 0; i < result_names.size(); i++) {
			if (null_values_index_set.contains(i)) {
				reorder_expressions.push_back(make_unique<BoundConstantExpression>(Value(result_types[i])));
			} else {
				reorder_expressions.push_back(make_unique<BoundColumnRefExpression>(
				    node.types[reorder_index[i]], ColumnBinding(prune_index, reorder_index[i])));
			}
		}

		if (node.type == QueryNodeType::SET_OPERATION_NODE) {
			auto &setop = (BoundSetOperationNode &)(node);
			setop.reorder_exprs = std::move(reorder_expressions);
		} else {
			D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
			auto &select_node = (BoundSelectNode &)(node);
			select_node.reorder_exprs = std::move(reorder_expressions);
		}
	}

#if 0
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		auto &setop = (BoundSetOperationNode &)(node);
		// First recursive to child
		// SELECT x FROM test_a UNION ALL SELECT y FROM test_b;
		// The name of query node of (SELECT y FROM test_b) will convert to x
		// So we should use brother names in this case.
		// setop.left_binder->PushProjection(*setop.left, result_names, result_types);
		// setop.right_binder->PushProjection(*setop.right, result_names, result_types);

		// case_insensitive_map_t<idx_t> node_names_map;
		// case_insensitive_map_t<idx_t> alias_names_map;

		/*
		for (idx_t i = 0; i < setop.names.size(); ++i) {
		    node_names_map[setop.names[i]] = i;
		}
		*/

		/*
		if (brother_names) {
		    for (idx_t i = 0; i < setop.names.size(); ++i) {
		        alias_names_map[(*brother_names)[i]] = i;
		    }
		}
		*/

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
			/*
			if (!name_exist && brother_names) {
			    name_index = alias_names_map.find(result_names[i]);
			    name_exist = name_index != alias_names_map.end();
			}
			*/
			if (name_exist) {
				new_names[i] = std::move(setop.names[name_index->second]);
				new_types[i] = std::move(setop.types[name_index->second]);
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
		// setop.need_reorder = need_reorder;
		// setop.reorder_map = std::move(reorder_index);
		setop.names = std::move(new_names);
		setop.types = std::move(new_types);

		if (need_reorder) {
			vector<unique_ptr<Expression>> reorder_expressions(result_names.size());
			for (idx_t i = 0; i < result_names.size(); i++) {
				if (null_values_index_set.contains(i)) {
					reorder_expressions.push_back(make_unique<BoundConstantExpression>(Value(result_types[i])));
				} else {
					reorder_expressions.push_back(make_unique<BoundColumnRefExpression>(
					    setop.types[reorder_index[i]], ColumnBinding(setop.setop_index, reorder_index[i])));
				}
			}
			setop.reorder_exprs = std::move(reorder_expressions);
		}

		// Rebind Modifiers
		// eg. (SELECT x FROM test_a UNION BY NAME SELECT y FROM test_b) ORDER BY y
		// The query node of (SELECT y FROM test_b) will convert to (SELECT NULL AS x, y FROM test_b)
		// So we should rebind modifiers
		/*
		if (!setop.modifiers.empty()) {
		    setop.modifiers.clear();
		    case_insensitive_map_t<idx_t> alias_map;
		    expression_map_t<idx_t> expression_map;
		    GatherAliases(node, alias_map, expression_map);
		    D_ASSERT(setop.unbound_node != nullptr);
		    SetOperationNode *statement = dynamic_cast<SetOperationNode *>(setop.unbound_node);
		    D_ASSERT(statement != nullptr);
		    // now we perform the actual resolution of the ORDER BY/DISTINCT expressions
		    OrderBinder order_binder({setop.left_binder.get(), setop.right_binder.get()}, setop.setop_index, alias_map,
		                             expression_map, statement->left->GetSelectList().size());
		    BindModifiers(order_binder, *statement, node);
		}
		setop.unbound_node = nullptr;
		*/
	} else {
		// SELECT NODE
		D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
		auto &select_node = (BoundSelectNode &)(node);
		// case_insensitive_map_t<idx_t> node_names_map;
		// case_insensitive_map_t<idx_t> alias_names_map;
		/*
		for (idx_t i = 0; i < select_node.names.size(); ++i) {
		    node_names_map[select_node.names[i]] = i;
		}
		*/

		/*
		if (brother_names) {
		    for (idx_t i = 0; i < select_node.names.size(); ++i) {
		        alias_names_map[(*brother_names)[i]] = i;
		    }
		}
		*/

		// vector<std::unique_ptr<Expression>> new_node_exprs(result_names.size());
		vector<LogicalType> new_types(result_names.size());
		vector<string> new_names(result_names.size());
		vector<idx_t> reorder_index(result_names.size());
		bool need_reorder = false;
		unordered_set<idx_t> null_values_index_set;
		// vector<unique_ptr<ParsedExpression>> new_original_exprs(result_names.size());
		for (idx_t i = 0; i < result_names.size(); ++i) {
			auto name_index = node_names_map.find(result_names[i]);
			bool name_exist = name_index != node_names_map.end();
			/*
			if (!name_exist && brother_names) {
			    name_index = alias_names_map.find(result_names[i]);
			    name_exist = name_index != alias_names_map.end();
			}
			*/
			if (name_exist) {
				new_names[i] = std::move(select_node.names[name_index->second]);
				new_types[i] = std::move(select_node.types[name_index->second]);
				need_reorder = need_reorder || name_index->second != i;
				reorder_index[i] = name_index->second;
				// new_node_exprs[i] = std::move(select_node.select_list[name_index->second]);
				// new_original_exprs[i] = std::move(select_node.original_expressions[name_index->second]);
			} else {
				new_names[i] = result_names[i];
				new_types[i] = result_types[i];
				need_reorder = true;
				reorder_index[i] = i;
				null_values_index_set.insert(i);
				// auto constant_null_expr = make_unique<BoundConstantExpression>(Value(result_types[i]));
				// new_node_exprs[i] = std::move(constant_null_expr);
				// new_original_exprs[i] = std::move(make_unique<ConstantExpression>(Value(result_types[i])));
			}
		}

		select_node.names = std::move(new_names);
		select_node.types = std::move(new_types);

		if (need_reorder) {
			vector<unique_ptr<Expression>> reorder_expressions(result_names.size());
			D_ASSERT(result_names.size() >= select_node.column_count);
			for (idx_t i = 0; i < result_names.size(); i++) {
				if (null_values_index_set.contains(i)) {
					reorder_expressions.push_back(make_unique<BoundConstantExpression>(Value(result_types[i])));
				} else {
					reorder_expressions.push_back(make_unique<BoundColumnRefExpression>(
					    select_node.types[reorder_index[i]],
					    ColumnBinding(select_node.projection_index, reorder_index[i])));
				}
			}
		}

		// select_node.original_expressions = std::move(new_original_exprs);
		// select_node.column_count = select_node.names.size();
		/*
		if (!select_node.modifiers.empty()) {
		    select_node.modifiers.clear();
		    case_insensitive_map_t<idx_t> alias_map;
		    expression_map_t<idx_t> expression_map;
		    GatherAliases(node, alias_map, expression_map);
		    D_ASSERT(select_node.unbound_node != nullptr);
		    SelectNode *statement = dynamic_cast<SelectNode *>(select_node.unbound_node);
		    D_ASSERT(statement != nullptr);
		    // now we perform the actual resolution of the ORDER BY/DISTINCT expressions
		    OrderBinder order_binder({select_node.binder.get()}, select_node.projection_index, alias_map,
		                             expression_map, node.names.size());
		    BindModifiers(order_binder, *statement, node);
		}
		*/

		// TODO(lokax): select_node.original_expressions...
		// select_node.unbound_node = nullptr;
	}
#endif
}

unique_ptr<BoundQueryNode> Binder::BindNode(SetOperationNode &statement) {
	auto result = make_unique<BoundSetOperationNode>();
	result->setop_type = statement.setop_type;
	// TODO(lokax): BindSelectNode函数还没有做
	// result->binder = shared_from_this();
	// result->unbound_node = &statement;
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

		// BoundSelectNode *left_node = dynamic_cast<BoundSelectNode *>(result->left.get());
		// BoundSelectNode *right_node = dynamic_cast<BoundSelectNode *>(result->right.get());
		// D_ASSERT(left_node != nullptr && right_node != nullptr);

		// 进来之后两个QueryNode

		//! V2只设置每个Node的相应的names和types
		BoundQueryNode *left_node = result->left.get();
		BoundQueryNode *right_node = result->right.get();

		for (idx_t i = 0; i < left_node->names.size(); ++i) {
			left_names_map[left_node->names[i]] = i;
		}

		for (idx_t i = 0; i < right_node->names.size(); ++i) {
			if (left_names_map.find(right_node->names[i]) == left_names_map.end()) {
				// auto constant_null_expr = make_unique<BoundConstantExpression>(Value(right_node->types[i]));
				// left_node->select_list.push_back(std::move(constant_null_expr));
				// Select List先不设置
				// left_node->names.push_back(right_node->names[i]);
				// left_node->types.push_back(right_node->types[i]);
				// left_node->column_count++;
				result->names.push_back(right_node->names[i]);
			}
			right_names_map[right_node->names[i]] = i;
		}

		idx_t new_size = result->names.size();
		// idx_t new_size = left_node->names.size();
		// std::vector<std::unique_ptr<Expression>> new_right_node_exprs(new_size);
		// vector<LogicalType> new_types(new_size);
		// vector<string> new_names(new_size);
		/*
		for (idx_t i = 0; i < new_size; ++i) {
		    auto iter = right_names_map.find(left_node->names[i]);
		    if (iter != right_names_map.end()) {
		        // new_right_node_exprs[i] = std::move(right_node->select_list[iter->second]);
		        new_types[i] = std::move(right_node->types[iter->second]);
		        new_names[i] = std::move(right_node->names[iter->second]);
		    } else {
		        auto constant_null_expr = make_unique<BoundConstantExpression>(Value(left_node->types[i]));
		        // new_right_node_exprs[i] = std::move(constant_null_expr);
		        new_names[i] = left_node->names[i];
		        new_types[i] = left_node->types[i];
		    }
		}
		*/

		// right_node->column_count = new_size;
		// right_node->names = std::move(new_names);
		// right_node->types = std::move(new_types);
		// right_node->select_list = std::move(new_right_node_exprs);

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
			/*
			auto result_type = LogicalType::MaxLogicalType(left_node->types[i], right_node->types[i]);
			if (!can_contain_nulls) {
			    if (ExpressionBinder::ContainsNullType(result_type)) {
			        result_type = ExpressionBinder::ExchangeNullType(result_type);
			    }
			}
			*/
			result->types.push_back(result_type);
		}

		PushProjection(*result->left, left_names_map, result->names, result->types);
		PushProjection(*result->right, right_names_map, result->names, result->types);

		//! TOOD(lokax): remove union by name root
		/*
		if (statement.union_by_name_root) {
		    /*
		    case_insensitive_map_t<idx_t> result_names_map;
		    for (idx_t i = 0; i < result->names.size(); ++i) {
		        result_names_map[result->names[i]] = i;
		    }
		    */
		// PushProjection(*result, result->names, nullptr, result->types);
		//}

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
