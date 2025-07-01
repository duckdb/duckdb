#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/expression/default_expression.hpp"

#include <algorithm>

namespace duckdb {

unique_ptr<BoundMergeIntoAction> Binder::BindMergeAction(TableCatalogEntry &table, idx_t proj_index,
                                                         vector<unique_ptr<Expression>> &expressions,
                                                         unique_ptr<LogicalOperator> &root, MergeIntoAction &action,
                                                         Binder &source_binder, LogicalOperator &source) {
	auto result = make_uniq<BoundMergeIntoAction>();
	result->action_type = action.action_type;
	if (action.condition) {
		WhereBinder where_binder(*this, context);
		auto cond = where_binder.Bind(action.condition);
		result->condition =
		    make_uniq<BoundColumnRefExpression>(cond->return_type, ColumnBinding(proj_index, expressions.size()));
		result->condition->alias = cond->ToString();
		expressions.push_back(std::move(cond));
	}
	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE:
		BindUpdateSet(proj_index, root, *action.update_info, table, result->columns, result->expressions, expressions);
		break;
	case MergeActionType::MERGE_INSERT: {
		vector<LogicalIndex> named_column_map;
		vector<LogicalType> expected_types;
		BindInsertColumnList(table, action.insert_columns, action.default_values, named_column_map, expected_types,
		                     result->column_index_map);

		vector<unique_ptr<Expression>> insert_expressions;
		if (!action.default_values && action.expressions.empty()) {
			// no expressions: *
			// expand source bindings
			source.ResolveOperatorTypes();
			auto column_bindings = source.GetColumnBindings();
			for (idx_t c = 0; c < column_bindings.size(); c++) {
				auto expr = make_uniq<BoundColumnRefExpression>(source.types[c], column_bindings[c]);
				insert_expressions.push_back(std::move(expr));
			}
		} else {
			// explicit expressions - plan them
			for (idx_t i = 0; i < action.expressions.size(); i++) {
				auto &column = table.GetColumns().GetColumn(named_column_map[i]);

				InsertBinder insert_binder(*this, context);
				insert_binder.target_type = column.Type();

				TryReplaceDefaultExpression(action.expressions[i], column);
				auto insert_expr = insert_binder.Bind(action.expressions[i]);

				PlanSubqueries(insert_expr, root);
				insert_expressions.push_back(std::move(insert_expr));
			}
		}
		for (auto &insert_expr : insert_expressions) {
			result->expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    insert_expr->return_type, ColumnBinding(proj_index, expressions.size())));
			expressions.push_back(std::move(insert_expr));
		}
		break;
	}
	case MergeActionType::MERGE_DELETE:
	case MergeActionType::MERGE_DO_NOTHING:
	case MergeActionType::MERGE_ABORT:
		// DELETE / DO NOTHING / ABORT have nothing extra to bind
		break;
	default:
		throw InternalException("Unsupported merge action type");
	}
	return result;
}

BoundStatement Binder::Bind(MergeIntoStatement &stmt) {
	JoinRef join;

	join.type = JoinType::LEFT;
	join.left = std::move(stmt.source);
	join.right = std::move(stmt.target);
	if (stmt.join_condition) {
		join.condition = std::move(stmt.join_condition);
	} else {
		join.using_columns = std::move(stmt.using_columns);
	}
	auto bound_join_node = Bind(join);
	auto &bound_join = bound_join_node->Cast<BoundJoinRef>();

	auto root = CreatePlan(bound_join);
	auto &source = *root->children[0];
	auto &get = root->children[1]->Cast<LogicalGet>();
	auto &table = *get.GetTable();

	if (!table.temporary) {
		// update of persistent table: not read only!
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
	}

	auto merge_into = make_uniq<LogicalMergeInto>(table);
	merge_into->table_index = GenerateTableIndex();

	// bind table constraints/default values in case these are referenced
	auto &catalog_name = table.ParentCatalog().GetName();
	auto &schema_name = table.ParentSchema().name;
	BindDefaultValues(table.GetColumns(), merge_into->bound_defaults, catalog_name, schema_name);
	merge_into->bound_constraints = BindConstraints(table);

	// bind the merge actions
	auto proj_index = GenerateTableIndex();
	vector<unique_ptr<Expression>> projection_expressions;

	for (auto &action : stmt.when_matched_actions) {
		merge_into->when_matched_actions.push_back(
		    BindMergeAction(table, proj_index, projection_expressions, root, *action, *bound_join.left_binder, source));
	}
	for (auto &action : stmt.when_not_matched_actions) {
		merge_into->when_not_matched_actions.push_back(
		    BindMergeAction(table, proj_index, projection_expressions, root, *action, *bound_join.left_binder, source));
	}

	// FIXME: need to handle when update is del and insert
	// bind any extra columns necessary for CHECK constraints or indexes
	//	table.BindUpdateConstraints(*this, *get, *proj, *update, context);

	merge_into->row_id_start = projection_expressions.size();
	// finally bind the row id column and add them to the projection list
	BindRowIdColumns(table, get, projection_expressions);

	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	proj->AddChild(std::move(root));

	merge_into->AddChild(std::move(proj));

	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};
	result.plan = std::move(merge_into);

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb
