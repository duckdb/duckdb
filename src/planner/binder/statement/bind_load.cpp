#include "duckdb/parser/statement/load_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include <algorithm>

namespace duckdb {

BoundStatement Binder::Bind(LoadStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_LOAD, std::move(stmt.info));
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
