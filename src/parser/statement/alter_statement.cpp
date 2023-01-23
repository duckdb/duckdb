#include "duckdb/parser/statement/alter_statement.hpp"

namespace duckdb {

AlterStatement::AlterStatement() : SQLStatement(StatementType::ALTER_STATEMENT) {
}

AlterStatement::AlterStatement(const AlterStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> AlterStatement::Copy() const {
	return unique_ptr<AlterStatement>(new AlterStatement(*this));
}

bool AlterStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const AlterStatement &)*other_p;
	D_ASSERT(info);
	return info->Equals(other.info.get());
}

} // namespace duckdb
