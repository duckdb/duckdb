#include "duckdb/parser/statement/transaction_statement.hpp"

namespace duckdb {

TransactionStatement::TransactionStatement(unique_ptr<TransactionInfo> info)
    : SQLStatement(StatementType::TRANSACTION_STATEMENT), info(std::move(info)) {
}

TransactionStatement::TransactionStatement(const TransactionStatement &other)
    : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> TransactionStatement::Copy() const {
	return unique_ptr<TransactionStatement>(new TransactionStatement(*this));
}

string TransactionStatement::ToString() const {
	return info->ToString();
}

} // namespace duckdb
