#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/returning_binder.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DeleteStatement &stmt) {
	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	auto root = std::move(bound_table.plan);
	if (root->type != LogicalOperatorType::LOGICAL_GET) {
		throw BinderException("Can only delete from base table");
	}
	auto &get = root->Cast<LogicalGet>();
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		throw BinderException("Can only delete from base table");
	}
	auto &table = *table_ptr;
	if (!table.temporary) {
		// delete from persistent table: not read only!
		auto &properties = GetStatementProperties();
		properties.RegisterDBModify(table.catalog, context, DatabaseModificationType::DELETE_DATA);
	}

	// plan any tables from the various using clauses
	if (!stmt.using_clauses.empty()) {
		unique_ptr<LogicalOperator> child_operator;
		for (auto &using_clause : stmt.using_clauses) {
			// bind the using clause
			auto using_binder = Binder::CreateBinder(context, this);
			auto op = using_binder->Bind(*using_clause);
			if (child_operator) {
				// already bound a child: create a cross product to unify the two
				child_operator = LogicalCrossProduct::Create(std::move(child_operator), std::move(op.plan));
			} else {
				child_operator = std::move(op.plan);
			}
			bind_context.AddContext(std::move(using_binder->bind_context));
		}
		if (child_operator) {
			root = LogicalCrossProduct::Create(std::move(root), std::move(child_operator));
		}
	}

	// project any additional columns required for the condition
	unique_ptr<Expression> condition;
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		condition = binder.Bind(stmt.condition);

		PlanSubqueries(condition, root);
		auto filter = make_uniq<LogicalFilter>(std::move(condition));
		filter->AddChild(std::move(root));
		root = std::move(filter);
	}
	// create the delete node
	auto del = make_uniq<LogicalDelete>(table, GenerateTableIndex());
	del->bound_constraints = BindConstraints(table);

	// If RETURNING is present, add all physical table columns to the scan so we can pass them through
	// instead of having to fetch them by row ID in PhysicalDelete.
	// Generated columns will be computed in the RETURNING projection by the binder.
	if (!stmt.returning_list.empty()) {
		auto &column_ids = get.GetColumnIds();
		auto &columns = table.GetColumns();
		auto physical_count = columns.PhysicalColumnCount();

		// Build a map from storage index -> input chunk index
		// return_columns[storage_idx] = input_chunk_idx
		del->return_columns.resize(physical_count, DConstants::INVALID_INDEX);

		// First, map columns already in the scan to their storage indices
		for (idx_t chunk_idx = 0; chunk_idx < column_ids.size(); chunk_idx++) {
			auto &col_id = column_ids[chunk_idx];
			if (col_id.IsVirtualColumn()) {
				continue;
			}
			// Get the column by logical index, then get its storage index
			auto logical_idx = col_id.GetPrimaryIndex();
			if (!columns.GetColumn(LogicalIndex(logical_idx)).Generated()) {
				auto storage_idx = columns.GetColumn(LogicalIndex(logical_idx)).StorageOid();
				del->return_columns[storage_idx] = chunk_idx;
			}
		}

		// Add any missing physical columns to the scan
		for (auto &col : columns.Physical()) {
			auto storage_idx = col.StorageOid();
			if (del->return_columns[storage_idx] == DConstants::INVALID_INDEX) {
				del->return_columns[storage_idx] = column_ids.size();
				get.AddColumnId(col.Logical().index);
			}
		}
	}

	del->AddChild(std::move(root));

	// bind the row id columns and add them to the projection list
	BindRowIdColumns(table, get, del->expressions);

	if (!stmt.returning_list.empty()) {
		del->return_chunk = true;

		auto update_table_index = GenerateTableIndex();
		del->table_index = update_table_index;

		unique_ptr<LogicalOperator> del_as_logicaloperator = std::move(del);
		return BindReturning(std::move(stmt.returning_list), table, stmt.table->alias, update_table_index,
		                     std::move(del_as_logicaloperator));
	}
	BoundStatement result;
	result.plan = std::move(del);
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::CHANGED_ROWS;

	return result;
}

} // namespace duckdb
