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

#include <algorithm>

namespace duckdb {

unique_ptr<BoundMergeIntoAction> Binder::BindMergeAction(TableCatalogEntry &table, idx_t proj_index,
                                                         vector<unique_ptr<Expression>> &expressions,
                                                         unique_ptr<LogicalOperator> &root, MergeIntoAction &action) {
	auto result = make_uniq<BoundMergeIntoAction>();
	result->action_type = action.action_type;
	if (action.condition) {
		WhereBinder where_binder(*this, context);
		auto cond = where_binder.Bind(action.condition);
		result->condition =
		    make_uniq<BoundColumnRefExpression>(cond->return_type, ColumnBinding(proj_index, expressions.size()));
		expressions.push_back(std::move(cond));
	}
	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE:
		BindUpdateSet(proj_index, root, *action.update_info, table, result->columns, result->expressions, expressions);
		break;
	case MergeActionType::MERGE_INSERT:
		if (!action.insert_columns.empty()) {
			throw InternalException("FIXME: insert columns");
		}
		// FIXME: this should be unified with binding INSERT
		// FIXME: perform column mapping
		for (idx_t i = 0; i < action.expressions.size(); i++) {
			InsertBinder insert_binder(*this, context);
			insert_binder.target_type = table.GetColumns().GetColumn(PhysicalIndex(i)).Type();
			auto insert_expr = insert_binder.Bind(action.expressions[i]);

			result->expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    insert_expr->return_type, ColumnBinding(proj_index, expressions.size())));
			expressions.push_back(std::move(insert_expr));
		}
		break;
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
	BoundStatement result;
	unique_ptr<LogicalOperator> root;

	// visit the table reference
	auto bound_table = Bind(*stmt.target);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}
	auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
	auto &table = table_binding.table;

	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	optional_ptr<LogicalGet> get;

	// create a right join between the target table and the merge source
	// we need to do a right join because we need to know all rows that matched, and all rows that did not match
	auto from_binder = Binder::CreateBinder(context, this);
	BoundJoinRef bound_join(JoinRefType::REGULAR);
	bound_join.type = JoinType::LEFT;
	bound_join.left = from_binder->Bind(*stmt.source);
	bound_join.right = std::move(bound_table);
	bind_context.AddContext(std::move(from_binder->bind_context));

	WhereBinder binder(*this, context);
	bound_join.condition = binder.Bind(stmt.join_condition);

	root = CreatePlan(bound_join);
	get = &root->children[1]->Cast<LogicalGet>();

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
		    BindMergeAction(table, proj_index, projection_expressions, root, *action));
	}
	for (auto &action : stmt.when_not_matched_actions) {
		merge_into->when_not_matched_actions.push_back(
		    BindMergeAction(table, proj_index, projection_expressions, root, *action));
	}

	// FIXME: need to handle when update is del and insert
	// bind any extra columns necessary for CHECK constraints or indexes
	//	table.BindUpdateConstraints(*this, *get, *proj, *update, context);

	merge_into->row_id_start = projection_expressions.size();
	// finally bind the row id column and add them to the projection list
	BindRowIdColumns(table, *get, projection_expressions);

	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	proj->AddChild(std::move(root));

	merge_into->AddChild(std::move(proj));

	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};
	result.plan = std::move(merge_into);

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb
