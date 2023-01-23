#include "duckdb/parser/statement/load_statement.hpp"

namespace duckdb {

LoadStatement::LoadStatement() : SQLStatement(StatementType::LOAD_STATEMENT) {
}

LoadStatement::LoadStatement(const LoadStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> LoadStatement::Copy() const {
	return unique_ptr<LoadStatement>(new LoadStatement(*this));
}

bool LoadStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const LoadStatement &)*other_p;
	D_ASSERT(info);
	return info->Equals(*other.info);
}

} // namespace duckdb
