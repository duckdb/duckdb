#include "duckdb/parser/statement/drop_statement.hpp"

namespace duckdb {

DropStatement::DropStatement() : SQLStatement(StatementType::DROP_STATEMENT), info(make_unique<DropInfo>()) {
}

DropStatement::DropStatement(const DropStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> DropStatement::Copy() const {
	return unique_ptr<DropStatement>(new DropStatement(*this));
}

bool DropStatement::Equals(const SQLStatement *other) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const DropStatement &)*other_p;
	return other.info->Equals(*info);
}

} // namespace duckdb
