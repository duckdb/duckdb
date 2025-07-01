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
#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include <algorithm>

namespace duckdb {

unique_ptr<BoundMergeIntoAction> Binder::BindMergeAction(LogicalMergeInto &merge_into, TableCatalogEntry &table,
                                                         LogicalGet &get, idx_t proj_index,
                                                         vector<unique_ptr<Expression>> &expressions,
                                                         unique_ptr<LogicalOperator> &root, MergeIntoAction &action,
                                                         LogicalOperator &source, const vector<string> &source_names,
                                                         const vector<LogicalType> &source_types) {
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
	result->bound_constraints = BindConstraints(table);
	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		if (!action.update_info) {
			// empty update list - generate it
			action.update_info = make_uniq<UpdateSetInfo>();
			if (action.column_order == InsertColumnOrder::INSERT_BY_NAME) {
				// UPDATE BY NAME - get the name list from the source binder
				action.update_info->columns = source_names;
			} else {
				// UPDATE BY POSITION - get the name list from the table
				for (auto &col : table.GetColumns().Physical()) {
					action.update_info->columns.push_back(col.Name());
				}
			}
			auto column_bindings = source.GetColumnBindings();
			if (column_bindings.size() != action.update_info->columns.size()) {
				throw BinderException(
				    "Data provided for UPDATE did not match column count in table - expected %d columns but got %d",
				    action.update_info->columns.size(), column_bindings.size());
			}
			for (idx_t c = 0; c < column_bindings.size(); c++) {
				auto expr = make_uniq<BoundColumnRefExpression>(source_types[c], column_bindings[c]);
				auto bound_expr = make_uniq<BoundExpression>(std::move(expr));
				action.update_info->expressions.push_back(std::move(bound_expr));
			}
		}
		BindUpdateSet(proj_index, root, *action.update_info, table, result->columns, result->expressions, expressions);

		// bind any additional columns that need to be bound for update constraints
		// FIXME: this is pretty hacky
		// construct a dummy projection and update
		LogicalProjection proj(proj_index, std::move(expressions));
		LogicalUpdate update(table);
		update.return_chunk = false;
		update.columns = std::move(result->columns);
		update.expressions = std::move(result->expressions);
		update.bound_defaults = std::move(merge_into.bound_defaults);
		update.bound_constraints = std::move(result->bound_constraints);
		update.update_is_del_and_insert = false;

		// call BindUpdateConstraints
		table.BindUpdateConstraints(*this, get, proj, update, context);

		// move all moved values back
		merge_into.bound_defaults = std::move(update.bound_defaults);
		expressions = std::move(proj.expressions);
		result->columns = std::move(update.columns);
		result->expressions = std::move(update.expressions);
		result->bound_constraints = std::move(update.bound_constraints);
		result->update_is_del_and_insert = update.update_is_del_and_insert;
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		if (action.column_order == InsertColumnOrder::INSERT_BY_NAME) {
			// INSERT BY NAME - get the name list from the source binder and push it into the table
			if (!action.insert_columns.empty()) {
				throw InternalException("INSERT BY NAME cannot be combined with a column list");
			}
			action.insert_columns = source_names;
		}
		vector<LogicalIndex> named_column_map;
		vector<LogicalType> expected_types;
		BindInsertColumnList(table, action.insert_columns, action.default_values, named_column_map, expected_types,
		                     result->column_index_map);

		vector<unique_ptr<Expression>> insert_expressions;
		if (!action.default_values && action.expressions.empty()) {
			// no expressions: *
			// expand source bindings
			auto column_bindings = source.GetColumnBindings();
			for (idx_t c = 0; c < column_bindings.size(); c++) {
				auto expr = make_uniq<BoundColumnRefExpression>(source_types[c], column_bindings[c]);
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
	case MergeActionType::MERGE_ERROR:
		// DELETE / DO NOTHING / ABORT have nothing extra to bind
		break;
	default:
		throw InternalException("Unsupported merge action type");
	}
	return result;
}

BoundStatement Binder::Bind(MergeIntoStatement &stmt) {
	JoinRef join;

	// bind the target table
	auto target_binder = Binder::CreateBinder(context, this);
	auto bound_table = target_binder->Bind(*stmt.target);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only merge into base tables!");
	}
	auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
	auto &table = table_binding.table;

	// bind the source
	auto source_binder = Binder::CreateBinder(context, this);
	auto source_binding = source_binder->Bind(*stmt.source);

	// get the source names/types
	vector<string> source_names;
	vector<LogicalType> source_types;
	source_binder->bind_context.GetTypesAndNames(source_names, source_types);

	// bind the join between the source and target
	join.type = JoinType::LEFT;
	join.left = make_uniq<BoundRefWrapper>(std::move(source_binding), std::move(source_binder));
	join.right = make_uniq<BoundRefWrapper>(std::move(bound_table), std::move(target_binder));
	if (stmt.join_condition) {
		join.condition = std::move(stmt.join_condition);
	} else {
		join.using_columns = std::move(stmt.using_columns);
	}
	auto bound_join_node = Bind(join);

	auto root = CreatePlan(*bound_join_node);
	auto &source = *root->children[0];
	auto &get = root->children[1]->Cast<LogicalGet>();

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

	// bind the merge actions
	auto proj_index = GenerateTableIndex();
	vector<unique_ptr<Expression>> projection_expressions;

	for (auto &action : stmt.when_matched_actions) {
		merge_into->when_matched_actions.push_back(BindMergeAction(*merge_into, table, get, proj_index,
		                                                           projection_expressions, root, *action, source,
		                                                           source_names, source_types));
	}
	for (auto &action : stmt.when_not_matched_actions) {
		merge_into->when_not_matched_actions.push_back(BindMergeAction(*merge_into, table, get, proj_index,
		                                                               projection_expressions, root, *action, source,
		                                                               source_names, source_types));
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
