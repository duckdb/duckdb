#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

BoundStatement Binder::Bind(VacuumStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::LOGICAL_VACUUM, move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
