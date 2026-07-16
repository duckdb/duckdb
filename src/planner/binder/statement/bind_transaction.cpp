#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_transaction.hpp"

namespace duckdb {

BoundStatement Binder::Bind(TransactionStatement &stmt) {
	auto &properties = GetStatementProperties();

	// Transaction statements do not require a valid transaction.
	properties.requires_valid_transaction = stmt.info->type == TransactionType::BEGIN_TRANSACTION;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalTransaction>(std::move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
