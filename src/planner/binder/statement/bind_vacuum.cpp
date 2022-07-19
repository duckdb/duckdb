#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

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
		auto ref = unique_ptr_cast<BoundTableRef, BoundBaseTableRef>(move(bound_table));
		stmt.info->table = ref->table;

		auto &columns = stmt.info->columns;
		vector<unique_ptr<Expression>> select_list;
		if (columns.empty()) {
			// Empty means ALL columns should be vacuumed/analyzed
			auto &get = (LogicalGet &)*ref->get;
			columns.insert(columns.end(), get.names.begin(), get.names.end());
		}
		for (auto &col_name : columns) {
			ColumnRefExpression colref(col_name, ref->table->name);
			auto result = bind_context.BindColumn(colref, 0);
			D_ASSERT(!result.HasError());
			select_list.push_back(move(result.expression));
		}
		auto table_scan = CreatePlan(*ref);
		D_ASSERT(table_scan->type == LogicalOperatorType::LOGICAL_GET);
		auto &get = (LogicalGet &)*table_scan;
		for (idx_t i = 0; i < get.column_ids.size(); i++) {
			stmt.info->column_id_map[i] = get.column_ids[i];
		}

		auto projection = make_unique<LogicalProjection>(GenerateTableIndex(), move(select_list));
		projection->children.push_back(move(table_scan));

		root = move(projection);
	}
	auto vacuum = make_unique<LogicalSimple>(LogicalOperatorType::LOGICAL_VACUUM, move(stmt.info));
	if (root) {
		vacuum->children.push_back(move(root));
	}

	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = move(vacuum);
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
