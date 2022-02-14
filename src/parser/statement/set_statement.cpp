#include "duckdb/parser/statement/set_statement.hpp"

namespace duckdb {

SetStatement::SetStatement(std::string name_p, Value value_p, SetScope scope_p)
    : SQLStatement(StatementType::SET_STATEMENT), name(move(name_p)), value(move(value_p)), scope(scope_p) {
}

unique_ptr<SQLStatement> SetStatement::Copy() const {
	return unique_ptr<SetStatement>(new SetStatement(*this));
}

} // namespace duckdb
