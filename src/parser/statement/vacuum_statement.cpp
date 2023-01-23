#include "duckdb/parser/statement/vacuum_statement.hpp"

namespace duckdb {

VacuumStatement::VacuumStatement(const VacuumOptions &options)
    : SQLStatement(StatementType::VACUUM_STATEMENT), info(make_unique<VacuumInfo>(options)) {
}

VacuumStatement::VacuumStatement(const VacuumStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> VacuumStatement::Copy() const {
	return unique_ptr<VacuumStatement>(new VacuumStatement(*this));
}

bool VacuumStatement::Equals(const SQLStatement *other_p) const {
	if (other_p->type != type) {
		return false;
	}
	auto &other = (const VacuumStatement &)*other_p;
	D_ASSERT(info);
	if (!info->Equals(*other.info)) {
		return false;
	}
	return true;
}

} // namespace duckdb
