#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

using namespace std;

namespace duckdb {

BoundStatement Binder::Bind(VacuumStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {SQLType::BOOLEAN};
	result.plan = make_unique<LogicalSimple>(LogicalOperatorType::VACUUM, move(stmt.info));
	return result;
}

} // namespace duckdb
