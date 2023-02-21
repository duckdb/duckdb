#include "duckdb/parser/statement/detach_statement.hpp"

namespace duckdb {

DetachStatement::DetachStatement() : SQLStatement(StatementType::DETACH_STATEMENT) {
}

DetachStatement::DetachStatement(const DetachStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

bool DetachStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const DetachStatement &)*other_p;
	return other.info->Equals(*info);
}

unique_ptr<SQLStatement> DetachStatement::Copy() const {
	return unique_ptr<DetachStatement>(new DetachStatement(*this));
}

} // namespace duckdb
