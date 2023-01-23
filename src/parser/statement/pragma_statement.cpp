#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {

PragmaStatement::PragmaStatement() : SQLStatement(StatementType::PRAGMA_STATEMENT), info(make_unique<PragmaInfo>()) {
}

PragmaStatement::PragmaStatement(const PragmaStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> PragmaStatement::Copy() const {
	return unique_ptr<PragmaStatement>(new PragmaStatement(*this));
}

bool PragmaStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const PragmaStatement &)*other_p;
	D_ASSERT(info);
	return info->Equals(*other.info);
}

} // namespace duckdb
