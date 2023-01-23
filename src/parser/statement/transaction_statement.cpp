#include "duckdb/parser/statement/transaction_statement.hpp"

namespace duckdb {

TransactionStatement::TransactionStatement(TransactionType type)
    : SQLStatement(StatementType::TRANSACTION_STATEMENT), info(make_unique<TransactionInfo>(type)) {
}

TransactionStatement::TransactionStatement(const TransactionStatement &other)
    : SQLStatement(other), info(make_unique<TransactionInfo>(other.info->type)) {
}

unique_ptr<SQLStatement> TransactionStatement::Copy() const {
	return unique_ptr<TransactionStatement>(new TransactionStatement(*this));
}

bool TransactionStatement::Equals(const SQLStatement *other_p) const {
	if (other->type != type) {
		return false;
	}
	auto &other = (const TransactionStatement &)*other_p;
	D_ASSERT(info);
	if (!info.equals(other.info.get())) {
		return false;
	}
	return true;
}

} // namespace duckdb
