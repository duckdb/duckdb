#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

BoundStatement Binder::Bind(VacuumStatement &stmt) {
	BoundStatement result;

	unique_ptr<LogicalOperator> root;

	if (stmt.info->has_table) {
		D_ASSERT(!stmt.info->table);
		D_ASSERT(stmt.info->column_id_map.empty());
		auto bound_table = Bind(*stmt.info->ref);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw InvalidInputException("Can only vacuum/analyze base tables!");
		}
		auto ref = unique_ptr_cast<BoundTableRef, BoundBaseTableRef>(std::move(bound_table));
		auto &table = ref->table;
		stmt.info->table = &table;

		auto &columns = stmt.info->columns;
		vector<unique_ptr<Expression>> select_list;
		if (columns.empty()) {
			// Empty means ALL columns should be vacuumed/analyzed
			auto &get = ref->get->Cast<LogicalGet>();
			columns.insert(columns.end(), get.names.begin(), get.names.end());
		}

		case_insensitive_set_t column_name_set;
		vector<string> non_generated_column_names;
		for (auto &col_name : columns) {
			if (column_name_set.count(col_name) > 0) {
				throw BinderException("Vacuum the same column twice(same name in column name list)");
			}
			column_name_set.insert(col_name);
			if (!table.ColumnExists(col_name)) {
				throw BinderException("Column with name \"%s\" does not exist", col_name);
			}
			auto &col = table.GetColumn(col_name);
			// ignore generated column
			if (col.Generated()) {
				continue;
			}
			non_generated_column_names.push_back(col_name);
			ColumnRefExpression colref(col_name, table.name);
			auto result = bind_context.BindColumn(colref, 0);
			if (result.HasError()) {
				throw BinderException(result.error);
			}
			select_list.push_back(std::move(result.expression));
		}
		stmt.info->columns = std::move(non_generated_column_names);
		if (!select_list.empty()) {
			auto table_scan = CreatePlan(*ref);
			D_ASSERT(table_scan->type == LogicalOperatorType::LOGICAL_GET);

			auto &get = table_scan->Cast<LogicalGet>();

			D_ASSERT(select_list.size() == get.column_ids.size());
			D_ASSERT(stmt.info->columns.size() == get.column_ids.size());
			for (idx_t i = 0; i < get.column_ids.size(); i++) {
				stmt.info->column_id_map[i] =
				    table.GetColumns().LogicalToPhysical(LogicalIndex(get.column_ids[i])).index;
			}

			auto projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(select_list));
			projection->children.push_back(std::move(table_scan));

			root = std::move(projection);
		} else {
			// eg. CREATE TABLE test (x AS (1));
			//     ANALYZE test;
			// Make it not a SINK so it doesn't have to do anything
			stmt.info->has_table = false;
		}
	}
	auto vacuum = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_VACUUM, std::move(stmt.info));
	if (root) {
		vacuum->children.push_back(std::move(root));
	}

	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = std::move(vacuum);
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
