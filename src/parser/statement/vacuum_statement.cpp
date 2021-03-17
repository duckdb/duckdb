#include "duckdb/parser/statement/vacuum_statement.hpp"

namespace duckdb {

VacuumStatement::VacuumStatement() : SQLStatement(StatementType::VACUUM_STATEMENT) {
}

unique_ptr<SQLStatement> VacuumStatement::Copy() const {
	return make_unique<VacuumStatement>();
}

} // namespace duckdb
