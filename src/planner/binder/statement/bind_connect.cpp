#include "duckdb/parser/statement/connect_execute_statement.hpp"
#include "duckdb/parser/statement/connect_statement.hpp"
#include "duckdb/parser/statement/disconnect_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ConnectStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_CONNECT, std::move(stmt.info));

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(DisconnectStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DISCONNECT, std::move(stmt.info));

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(ConnectExecuteStatement &stmt) {
	// CONNECT_EXECUTE is a marker the iterator/Query() loop scoops up to set a one-shot dispatch
	// target. Binding it produces a no-op LogicalSimple plan so it can flow through the existing
	// prepared-statement / planning machinery; the loop in ClientContext::Query() is what
	// actually consumes the marker and routes the following peel.
	auto info = make_uniq<ConnectInfo>();
	info->name = stmt.target;

	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_CONNECT_EXECUTE, std::move(info));

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
