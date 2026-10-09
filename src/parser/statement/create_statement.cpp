#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

CreateStatement::CreateStatement() : SQLStatement(StatementType::CREATE_STATEMENT) {
}

CreateStatement::CreateStatement(const CreateStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> CreateStatement::Copy() const {
	return unique_ptr<CreateStatement>(new CreateStatement(*this));
}

string CreateStatement::ToString() const {
	SQLRenderContext context;
	return ToString(context);
}

string CreateStatement::ToString(SQLRenderContext &context) const {
	return info->ToString(context);
}

} // namespace duckdb
