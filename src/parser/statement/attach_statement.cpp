#include "duckdb/parser/statement/attach_statement.hpp"

namespace duckdb {

AttachStatement::AttachStatement() : SQLStatement(StatementType::ATTACH_STATEMENT) {
}

AttachStatement::AttachStatement(const AttachStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> AttachStatement::Copy() const {
	return unique_ptr<AttachStatement>(new AttachStatement(*this));
}

bool AttachStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const AttachStatement &)*other_p;
	D_ASSERT(info);
	return info->Equals(other.info.get());
}

} // namespace duckdb
