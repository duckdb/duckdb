#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression_binder/update_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/data_table.hpp"

#include <algorithm>

namespace duckdb {

// This creates a LogicalProjection and moves 'root' into it as a child
// unless there are no expressions to project, in which case it just returns 'root'
unique_ptr<LogicalOperator> Binder::BindUpdateSet(LogicalOperator &op, unique_ptr<LogicalOperator> root,
                                                  UpdateSetInfo &set_info, TableCatalogEntry &table,
                                                  vector<PhysicalIndex> &columns) {
	auto proj_index = GenerateTableIndex();

	vector<unique_ptr<Expression>> projection_expressions;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());
	for (idx_t i = 0; i < set_info.columns.size(); i++) {
		auto &colname = set_info.columns[i];
		auto &expr = set_info.expressions[i];
		if (!table.ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname);
		}
		auto &column = table.GetColumn(colname);
		if (column.Generated()) {
			throw BinderException("Cant update column \"%s\" because it is a generated column!", column.Name());
		}
		if (std::find(columns.begin(), columns.end(), column.Physical()) != columns.end()) {
			throw BinderException("Multiple assignments to same column \"%s\"", colname);
		}
		columns.push_back(column.Physical());
		if (expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			op.expressions.push_back(make_uniq<BoundDefaultExpression>(column.Type()));
		} else {
			UpdateBinder binder(*this, context);
			binder.target_type = column.Type();
			auto bound_expr = binder.Bind(expr);
			PlanSubqueries(bound_expr, root);

			op.expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    bound_expr->return_type, ColumnBinding(proj_index, projection_expressions.size())));
			projection_expressions.push_back(std::move(bound_expr));
		}
	}
	if (op.type != LogicalOperatorType::LOGICAL_UPDATE && projection_expressions.empty()) {
		return root;
	}
	// now create the projection
	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	proj->AddChild(std::move(root));
	return unique_ptr_cast<LogicalProjection, LogicalOperator>(std::move(proj));
}

BoundStatement Binder::Bind(UpdateStatement &stmt) {
	BoundStatement result;
	unique_ptr<LogicalOperator> root;

	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}
	auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
	auto &table = table_binding.table;

	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	optional_ptr<LogicalGet> get;
	if (stmt.from_table) {
		auto from_binder = Binder::CreateBinder(context, this);
		BoundJoinRef bound_crossproduct(JoinRefType::CROSS);
		bound_crossproduct.left = std::move(bound_table);
		bound_crossproduct.right = from_binder->Bind(*stmt.from_table);
		root = CreatePlan(bound_crossproduct);
		get = &root->children[0]->Cast<LogicalGet>();
		bind_context.AddContext(std::move(from_binder->bind_context));
	} else {
		root = CreatePlan(*bound_table);
		get = &root->Cast<LogicalGet>();
	}

	if (!table.temporary) {
		// update of persistent table: not read only!
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context);
	}
	auto update = make_uniq<LogicalUpdate>(table);

	// set return_chunk boolean early because it needs uses update_is_del_and_insert logic
	if (!stmt.returning_list.empty()) {
		update->return_chunk = true;
	}
	// bind the default values
	auto &catalog_name = table.ParentCatalog().GetName();
	auto &schema_name = table.ParentSchema().name;
	BindDefaultValues(table.GetColumns(), update->bound_defaults, catalog_name, schema_name);
	update->bound_constraints = BindConstraints(table);

	// project any additional columns required for the condition/expressions
	if (stmt.set_info->condition) {
		WhereBinder binder(*this, context);
		auto condition = binder.Bind(stmt.set_info->condition);

		PlanSubqueries(condition, root);
		auto filter = make_uniq<LogicalFilter>(std::move(condition));
		filter->AddChild(std::move(root));
		root = std::move(filter);
	}

	D_ASSERT(stmt.set_info);
	D_ASSERT(stmt.set_info->columns.size() == stmt.set_info->expressions.size());

	auto proj_tmp = BindUpdateSet(*update, std::move(root), *stmt.set_info, table, update->columns);
	D_ASSERT(proj_tmp->type == LogicalOperatorType::LOGICAL_PROJECTION);
	auto proj = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(proj_tmp));

	// bind any extra columns necessary for CHECK constraints or indexes
	table.BindUpdateConstraints(*this, *get, *proj, *update, context);

	// finally add the row id column to the projection list
	auto virtual_columns = table.GetVirtualColumns();
	auto row_id_entry = virtual_columns.find(COLUMN_IDENTIFIER_ROW_ID);
	if (row_id_entry == virtual_columns.end()) {
		throw InternalException("BindDelete could not find the row id column in the virtual columns list of the table");
	}
	auto &column_ids = get->GetColumnIds();
	proj->expressions.push_back(make_uniq<BoundColumnRefExpression>(
	    row_id_entry->second.type, ColumnBinding(get->table_index, column_ids.size())));
	get->AddColumnId(COLUMN_IDENTIFIER_ROW_ID);

	// set the projection as child of the update node and finalize the result
	update->AddChild(std::move(proj));

	auto update_table_index = GenerateTableIndex();
	update->table_index = update_table_index;
	if (!stmt.returning_list.empty()) {
		unique_ptr<LogicalOperator> update_as_logicaloperator = std::move(update);

		return BindReturning(std::move(stmt.returning_list), table, stmt.table->alias, update_table_index,
		                     std::move(update_as_logicaloperator), std::move(result));
	}

	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};
	result.plan = std::move(update);

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb
