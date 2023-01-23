#include "duckdb/parser/statement/show_statement.hpp"

namespace duckdb {

ShowStatement::ShowStatement() : SQLStatement(StatementType::SHOW_STATEMENT), info(make_unique<ShowSelectInfo>()) {
}

ShowStatement::ShowStatement(const ShowStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> ShowStatement::Copy() const {
	return unique_ptr<ShowStatement>(new ShowStatement(*this));
}

bool ShowStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const ShowStatement &)*other_p;
	D_ASSERT(info);
	if (!info->Equals(*other.info)) {
		return false;
	}
	return true;
}

} // namespace duckdb
