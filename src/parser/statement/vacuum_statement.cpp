#include "duckdb/parser/statement/vacuum_statement.hpp"

namespace duckdb {

VacuumStatement::VacuumStatement() : SQLStatement(StatementType::VACUUM_STATEMENT) {
}

unique_ptr<SQLStatement> VacuumStatement::Copy() const {
	return unique_ptr<VacuumStatement>(new VacuumStatement(*this));
}

} // namespace duckdb
