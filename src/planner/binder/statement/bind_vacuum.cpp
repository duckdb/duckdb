#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

BoundStatement Binder::Bind(VacuumStatement &stmt) {
	BoundStatement result;

	if (stmt.info->has_table) {
		auto bound_table = Bind(*stmt.info->ref);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw InvalidInputException("Can only vacuum/analyze base tables!");
		}
		stmt.info->bound_ref = unique_ptr_cast<BoundTableRef, BoundBaseTableRef>(move(bound_table));

		auto &columns = stmt.info->columns;
		if (columns.empty()) {
			// Empty means ALL columns should be vacuumed/analyzed
			auto &get = (LogicalGet &)*stmt.info->bound_ref->get;
			columns.insert(columns.end(), get.names.begin(), get.names.end());
		}
	}

	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::LOGICAL_VACUUM, move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
