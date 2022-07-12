#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"

namespace duckdb {

BoundStatement Binder::Bind(PrepareStatement &stmt) {
	Planner prepared_planner(context);
	auto prepared_data = prepared_planner.PrepareSQLStatement(move(stmt.statement));

	auto prepare = make_unique<LogicalPrepare>(stmt.name, move(prepared_data), move(prepared_planner.plan));
	// we can prepare in read-only mode: prepared statements are not written to the catalog
	properties.read_only = true;
	// we can always prepare, even if the transaction has been invalidated
	// this is required because most clients ALWAYS invoke prepared statements
	properties.requires_valid_transaction = false;
	properties.allow_stream_result = false;
	properties.bound_all_parameters = true;
	properties.parameter_count = 0;
	properties.return_type = StatementReturnType::NOTHING;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = move(prepare);
	return result;
}

} // namespace duckdb
