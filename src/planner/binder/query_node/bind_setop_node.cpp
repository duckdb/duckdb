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
#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

struct SetOpAliasGatherer {
public:
	explicit SetOpAliasGatherer(SelectBindState &bind_state_p) : bind_state(bind_state_p) {
	}

	void GatherAliases(BoundStatement &stmt, const vector<idx_t> &reorder_idx);
	void GatherSetOpAliases(BoundStatement &stmt, const vector<idx_t> &reorder_idx);

private:
	SelectBindState &bind_state;
};

void SetOpAliasGatherer::GatherAliases(BoundStatement &stmt, const vector<idx_t> &reorder_idx) {
	if (stmt.extra_info.setop_type != SetOperationType::NONE) {
		GatherSetOpAliases(stmt, reorder_idx);
		return;
	}

	// query node
	auto &select_names = stmt.names;
	// fill the alias lists with the names
	D_ASSERT(reorder_idx.size() == select_names.size());
	for (idx_t i = 0; i < select_names.size(); i++) {
		auto &name = select_names[i];
		// first check if the alias is already in there
		auto entry = bind_state.alias_map.find(name);

		idx_t index = reorder_idx[i];

		if (entry == bind_state.alias_map.end()) {
			// the alias is not in there yet, just assign it
			bind_state.alias_map[name] = index;
		}
	}
	// check if the expression matches one of the expressions in the original expression list
	auto &select_list = stmt.extra_info.original_expressions;
	for (idx_t i = 0; i < select_list.size(); i++) {
		auto &expr = select_list[i];
		idx_t index = reorder_idx[i];
		// now check if the node is already in the set of expressions
		auto expr_entry = bind_state.projection_map.find(*expr);
		if (expr_entry != bind_state.projection_map.end()) {
			// the node is in there
			// repeat the same as with the alias: if there is an ambiguity we insert "-1"
			if (expr_entry->second != index) {
				bind_state.projection_map[*expr] = DConstants::INVALID_INDEX;
			}
		} else {
			// not in there yet, just place it in there
			bind_state.projection_map[*expr] = index;
		}
	}
}

void SetOpAliasGatherer::GatherSetOpAliases(BoundStatement &stmt, const vector<idx_t> &reorder_idx) {
	// create new reorder index
	auto &extra_info = stmt.extra_info;
	auto setop_type = extra_info.setop_type;
	if (setop_type == SetOperationType::UNION_BY_NAME) {
		auto &setop_names = stmt.names;
		// for UNION BY NAME - create a new re-order index
		case_insensitive_map_t<idx_t> reorder_map;
		for (idx_t col_idx = 0; col_idx < setop_names.size(); ++col_idx) {
			reorder_map[setop_names[col_idx]] = reorder_idx[col_idx];
		}

		// use new reorder index
		for (auto &child : extra_info.bound_children) {
			vector<idx_t> new_reorder_idx;
			auto &child_names = child.names;
			for (idx_t col_idx = 0; col_idx < child_names.size(); col_idx++) {
				auto &col_name = child_names[col_idx];
				auto entry = reorder_map.find(col_name);
				if (entry == reorder_map.end()) {
					throw InternalException("SetOp - Column name not found in reorder_map in UNION BY NAME");
				}
				new_reorder_idx.push_back(entry->second);
			}
			GatherAliases(child, new_reorder_idx);
		}
	} else {
		for (auto &child : extra_info.bound_children) {
			GatherAliases(child, reorder_idx);
		}
	}
}

static void GatherAliases(BoundSetOperationNode &root, BoundStatement &stmt, SelectBindState &bind_state) {
	SetOpAliasGatherer gatherer(bind_state);
	vector<idx_t> reorder_idx;
	for (idx_t i = 0; i < root.names.size(); i++) {
		reorder_idx.push_back(i);
	}
	gatherer.GatherAliases(stmt, reorder_idx);
}

void Binder::BuildUnionByNameInfo(BoundSetOperationNode &result) {
	D_ASSERT(result.setop_type == SetOperationType::UNION_BY_NAME);
	vector<case_insensitive_map_t<idx_t>> node_name_maps;
	case_insensitive_set_t global_name_set;

	// Build a name_map to use to check if a name exists
	// We throw a binder exception if two same name in the SELECT list
	D_ASSERT(result.names.empty());
	for (auto &child : result.bound_children) {
		auto &child_names = child.node.names;
		case_insensitive_map_t<idx_t> node_name_map;
		for (idx_t i = 0; i < child_names.size(); ++i) {
			auto &col_name = child_names[i];
			if (node_name_map.find(col_name) != node_name_map.end()) {
				throw BinderException(
				    "UNION (ALL) BY NAME operation doesn't support duplicate names in the SELECT list - "
				    "the name \"%s\" occurs multiple times",
				    col_name);
			}
			if (global_name_set.find(col_name) == global_name_set.end()) {
				// column is not yet present in the result
				result.names.push_back(col_name);
				global_name_set.insert(col_name);
			}
			node_name_map[col_name] = i;
		}
		node_name_maps.push_back(std::move(node_name_map));
	}

	idx_t new_size = result.names.size();
	bool need_reorder = false;

	// construct the return type of each of the columns
	for (idx_t i = 0; i < new_size; ++i) {
		auto &col_name = result.names[i];
		LogicalType result_type(LogicalTypeId::INVALID);
		for (idx_t child_idx = 0; child_idx < result.bound_children.size(); ++child_idx) {
			auto &child_types = result.bound_children[child_idx].node.types;
			auto &child_name_map = node_name_maps[child_idx];
			// check if the column exists in this child node
			auto entry = child_name_map.find(col_name);
			if (entry == child_name_map.end()) {
				need_reorder = true;
			} else {
				auto col_idx_in_child = entry->second;
				auto &child_col_type = child_types[col_idx_in_child];
				// the child exists in this node - compute the type
				if (result_type.id() == LogicalTypeId::INVALID) {
					result_type = child_col_type;
				} else {
					result_type = LogicalType::ForceMaxLogicalType(result_type, child_col_type);
				}
				if (i != col_idx_in_child) {
					// the column exists - but the children are out-of-order, so we need to re-order anyway
					need_reorder = true;
				}
			}
		}
		// compute the final type for each column
		if (!can_contain_nulls) {
			if (ExpressionBinder::ContainsNullType(result_type)) {
				result_type = ExpressionBinder::ExchangeNullType(result_type);
			}
		}
		result.types.push_back(result_type);
	}

	if (!need_reorder) {
		// if all columns in the children of the set-operations are identical we don't need to re-order at all
		// skip adding expressions entirely
		return;
	}
	// If reorder is required, generate the expressions for each node
	vector<vector<unique_ptr<Expression>>> reorder_expressions;
	reorder_expressions.resize(result.bound_children.size());
	for (idx_t i = 0; i < new_size; ++i) {
		auto &col_name = result.names[i];
		for (idx_t child_idx = 0; child_idx < result.bound_children.size(); ++child_idx) {
			auto &child = result.bound_children[child_idx];
			auto &child_name_map = node_name_maps[child_idx];
			// check if the column exists in this child node
			auto entry = child_name_map.find(col_name);
			unique_ptr<Expression> expr;
			if (entry == child_name_map.end()) {
				// the column does not exist - push a `NULL`
				expr = make_uniq<BoundConstantExpression>(Value(result.types[i]));
			} else {
				// the column exists - reference it
				auto col_idx_in_child = entry->second;
				auto &child_col_type = child.node.types[col_idx_in_child];
				auto root_idx = child.node.plan->GetRootIndex();
				expr = make_uniq<BoundColumnRefExpression>(child_col_type,
				                                           ColumnBinding(root_idx, col_idx_in_child));
			}
			reorder_expressions[child_idx].push_back(std::move(expr));
		}
	}
	// now push projections for each node
	for (idx_t child_idx = 0; child_idx < result.bound_children.size(); ++child_idx) {
		auto &child = result.bound_children[child_idx];
		auto &child_reorder_expressions = reorder_expressions[child_idx];
		// if we have re-order expressions push a projection
		vector<LogicalType> child_types;
		for (auto &expr : child_reorder_expressions) {
			child_types.push_back(expr->return_type);
		}
		auto child_projection =
			make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(child_reorder_expressions));
		child_projection->children.push_back(std::move(child.node.plan));
		child.node.plan = std::move(child_projection);
		child.node.types = std::move(child_types);
		child.node.names = result.names;
	}
}

BoundSetOpChild Binder::BindSetOpChild(QueryNode &child) {
	BoundSetOpChild bound_child;
	bound_child.binder = Binder::CreateBinder(context, this);
	bound_child.binder->can_contain_nulls = true;
	bound_child.node = bound_child.binder->BindNode(child);
	MoveCorrelatedExpressions(*bound_child.binder);
	return bound_child;
}

static void GatherSetOpBinders(BoundStatement &stmt, vector<reference<Binder>> &binders) {
	for(auto &child_binder : stmt.extra_info.child_binders) {
		binders.push_back(*child_binder);
	}
	for(auto &child_node : stmt.extra_info.bound_children) {
		GatherSetOpBinders(child_node, binders);
	}
}

BoundStatement Binder::BindNode(SetOperationNode &statement) {
	BoundSetOperationNode result;
	result.setop_type = statement.setop_type;
	result.setop_all = statement.setop_all;

	// first recursively visit the set operations
	// all children have an independent BindContext and Binder
	result.setop_index = GenerateTableIndex();
	if (statement.children.size() < 2) {
		throw InternalException("Set Operations must have at least 2 children");
	}
	if (statement.children.size() != 2 && statement.setop_type != SetOperationType::UNION &&
	    statement.setop_type != SetOperationType::UNION_BY_NAME) {
		throw InternalException("Set Operation type must have exactly 2 children - except for UNION/UNION_BY_NAME");
	}
	for (auto &child : statement.children) {
		result.bound_children.push_back(BindSetOpChild(*child));
	}

	if (result.setop_type == SetOperationType::UNION_BY_NAME) {
		// UNION BY NAME - merge the columns from all sides
		BuildUnionByNameInfo(result);
	} else {
		// UNION ALL BY POSITION - the columns of both sides must match exactly
		result.names = result.bound_children[0].node.names;
		auto result_columns = result.bound_children[0].node.types.size();
		for (idx_t i = 1; i < result.bound_children.size(); ++i) {
			if (result.bound_children[i].node.types.size() != result_columns) {
				throw BinderException("Set operations can only apply to expressions with the "
				                      "same number of result columns");
			}
		}

		// figure out the types of the setop result by picking the max of both
		for (idx_t i = 0; i < result_columns; i++) {
			auto result_type = result.bound_children[0].node.types[i];
			for (idx_t child_idx = 1; child_idx < result.bound_children.size(); ++child_idx) {
				auto &child_types = result.bound_children[child_idx].node.types;
				result_type = LogicalType::ForceMaxLogicalType(result_type, child_types[i]);
			}
			if (!can_contain_nulls) {
				if (ExpressionBinder::ContainsNullType(result_type)) {
					result_type = ExpressionBinder::ExchangeNullType(result_type);
				}
			}
			result.types.push_back(result_type);
		}
	}

	SelectBindState bind_state;
	if (!statement.modifiers.empty()) {
		// handle the ORDER BY/DISTINCT clauses
		vector<reference<Binder>> binders;
		for (auto &child : result.bound_children) {
			binders.push_back(*child.binder);
			GatherSetOpBinders(child.node, binders);
			GatherAliases(result, child.node, bind_state);
		}

		// now we perform the actual resolution of the ORDER BY/DISTINCT expressions
		OrderBinder order_binder(binders, bind_state);
		PrepareModifiers(order_binder, statement, result);
	}

	// finally bind the types of the ORDER/DISTINCT clause expressions
	BindModifiers(result, result.setop_index, result.names, result.types, bind_state);

	BoundStatement result_statement;
	result_statement.types = result.types;
	result_statement.names = result.names;
	result_statement.plan = CreatePlan(result);
	result_statement.extra_info.setop_type = statement.setop_type;
	for(auto &child : result.bound_children) {
		result_statement.extra_info.child_binders.push_back(std::move(child.binder));
		result_statement.extra_info.bound_children.push_back(std::move(child.node));
	}
	return result_statement;
}

} // namespace duckdb
