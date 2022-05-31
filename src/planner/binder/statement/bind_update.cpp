#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression_binder/update_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"
#include <algorithm>

namespace duckdb {

static void BindExtraColumns(TableCatalogEntry &table, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
                             unordered_set<column_t> &bound_columns) {
	if (bound_columns.size() <= 1) {
		return;
	}
	idx_t found_column_count = 0;
	unordered_set<idx_t> found_columns;
	for (idx_t i = 0; i < update.columns.size(); i++) {
		if (bound_columns.find(update.columns[i]) != bound_columns.end()) {
			// this column is referenced in the CHECK constraint
			found_column_count++;
			found_columns.insert(update.columns[i]);
		}
	}
	if (found_column_count > 0 && found_column_count != bound_columns.size()) {
		// columns in this CHECK constraint were referenced, but not all were part of the UPDATE
		// add them to the scan and update set
		for (auto &check_column_id : bound_columns) {
			if (found_columns.find(check_column_id) != found_columns.end()) {
				// column is already projected
				continue;
			}
			// column is not projected yet: project it by adding the clause "i=i" to the set of updated columns
			auto &column = table.columns[check_column_id];
			update.expressions.push_back(make_unique<BoundColumnRefExpression>(
			    column.Type(), ColumnBinding(proj.table_index, proj.expressions.size())));
			proj.expressions.push_back(make_unique<BoundColumnRefExpression>(
			    column.Type(), ColumnBinding(get.table_index, get.column_ids.size())));
			get.column_ids.push_back(check_column_id);
			update.columns.push_back(check_column_id);
		}
	}
}

static bool TypeSupportsRegularUpdate(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		// lists and maps don't support updates directly
		return false;
	case LogicalTypeId::STRUCT: {
		auto &child_types = StructType::GetChildTypes(type);
		for (auto &entry : child_types) {
			if (!TypeSupportsRegularUpdate(entry.second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

static void BindUpdateConstraints(TableCatalogEntry &table, LogicalGet &get, LogicalProjection &proj,
                                  LogicalUpdate &update) {
	// check the constraints and indexes of the table to see if we need to project any additional columns
	// we do this for indexes with multiple columns and CHECK constraints in the UPDATE clause
	// suppose we have a constraint CHECK(i + j < 10); now we need both i and j to check the constraint
	// if we are only updating one of the two columns we add the other one to the UPDATE set
	// with a "useless" update (i.e. i=i) so we can verify that the CHECK constraint is not violated
	for (auto &constraint : table.bound_constraints) {
		if (constraint->type == ConstraintType::CHECK) {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			// check constraint! check if we need to add any extra columns to the UPDATE clause
			BindExtraColumns(table, get, proj, update, check.bound_columns);
		}
	}
	// for index updates we always turn any update into an insert and a delete
	// we thus need all the columns to be available, hence we check if the update touches any index columns
	// If the returning keyword is used, we need access to the whole row in case the user requests it.
	// Therefore switch the update to a delete and insert.
	update.update_is_del_and_insert = false;
	table.storage->info->indexes.Scan([&](Index &index) {
		if (index.IndexIsUpdated(update.columns)) {
			update.update_is_del_and_insert = true;
			return true;
		}
		return false;
	});

	// we also convert any updates on LIST columns into delete + insert
	for (auto &col : update.columns) {
		if (!TypeSupportsRegularUpdate(table.columns[col].Type())) {
			update.update_is_del_and_insert = true;
			break;
		}
	}

	if (update.update_is_del_and_insert || update.return_chunk) {
		// the update updates a column required by an index or requires returning the updated rows,
		// push projections for all columns
		unordered_set<column_t> all_columns;
		for (idx_t i = 0; i < table.storage->column_definitions.size(); i++) {
			all_columns.insert(i);
		}
		BindExtraColumns(table, get, proj, update, all_columns);
	}
}

BoundStatement Binder::Bind(UpdateStatement &stmt) {
	BoundStatement result;
	unique_ptr<LogicalOperator> root;
	LogicalGet *get;

	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}
	auto &table_binding = (BoundBaseTableRef &)*bound_table;
	auto table = table_binding.table;

	if (stmt.from_table) {
		BoundCrossProductRef bound_crossproduct;
		bound_crossproduct.left = move(bound_table);
		bound_crossproduct.right = Bind(*stmt.from_table);
		root = CreatePlan(bound_crossproduct);
		get = (LogicalGet *)root->children[0].get();
	} else {
		root = CreatePlan(*bound_table);
		get = (LogicalGet *)root.get();
	}

	if (!table->temporary) {
		// update of persistent table: not read only!
		properties.read_only = false;
	}
	auto update = make_unique<LogicalUpdate>(table);

	// set return_chunk boolean early because it needs uses update_is_del_and_insert logic
	if (!stmt.returning_list.empty()) {
		update->return_chunk = true;
	}
	// bind the default values
	BindDefaultValues(table->columns, update->bound_defaults);

	// project any additional columns required for the condition/expressions
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		auto condition = binder.Bind(stmt.condition);

		PlanSubqueries(&condition, &root);
		auto filter = make_unique<LogicalFilter>(move(condition));
		filter->AddChild(move(root));
		root = move(filter);
	}

	D_ASSERT(stmt.columns.size() == stmt.expressions.size());

	auto proj_index = GenerateTableIndex();
	vector<unique_ptr<Expression>> projection_expressions;

	for (idx_t i = 0; i < stmt.columns.size(); i++) {
		auto &colname = stmt.columns[i];
		auto &expr = stmt.expressions[i];
		if (!table->ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname);
		}
		auto &column = table->GetColumn(colname);
		if (column.Generated()) {
			throw BinderException("Cant update column \"%s\" because it is a generated column!", column.Name());
		}
		if (std::find(update->columns.begin(), update->columns.end(), column.Oid()) != update->columns.end()) {
			throw BinderException("Multiple assignments to same column \"%s\"", colname);
		}
		update->columns.push_back(column.Oid());

		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			update->expressions.push_back(make_unique<BoundDefaultExpression>(column.Type()));
		} else {
			UpdateBinder binder(*this, context);
			binder.target_type = column.Type();
			auto bound_expr = binder.Bind(expr);
			PlanSubqueries(&bound_expr, &root);

			update->expressions.push_back(make_unique<BoundColumnRefExpression>(
			    bound_expr->return_type, ColumnBinding(proj_index, projection_expressions.size())));
			projection_expressions.push_back(move(bound_expr));
		}
	}

	// now create the projection
	auto proj = make_unique<LogicalProjection>(proj_index, move(projection_expressions));
	proj->AddChild(move(root));

	// bind any extra columns necessary for CHECK constraints or indexes
	BindUpdateConstraints(*table, *get, *proj, *update);

	// finally add the row id column to the projection list
	proj->expressions.push_back(make_unique<BoundColumnRefExpression>(
	    LogicalType::ROW_TYPE, ColumnBinding(get->table_index, get->column_ids.size())));
	get->column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);

	// set the projection as child of the update node and finalize the result
	update->AddChild(move(proj));

	if (!stmt.returning_list.empty()) {
		auto update_table_index = GenerateTableIndex();
		update->table_index = update_table_index;
		unique_ptr<LogicalOperator> update_as_logicaloperator = move(update);

		return BindReturning(move(stmt.returning_list), table, update_table_index, move(update_as_logicaloperator),
		                     move(result));

	} else {
		update->table_index = 0;
		result.names = {"Count"};
		result.types = {LogicalType::BIGINT};
		result.plan = move(update);
		properties.allow_stream_result = false;
		properties.return_type = StatementReturnType::CHANGED_ROWS;
	}
	return result;
}

} // namespace duckdb
