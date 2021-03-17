#include "duckdb/parser/statement/export_statement.hpp"

namespace duckdb {

ExportStatement::ExportStatement(unique_ptr<CopyInfo> info)
    : SQLStatement(StatementType::EXPORT_STATEMENT), info(move(info)) {
}

unique_ptr<SQLStatement> ExportStatement::Copy() const {
	return make_unique_base<SQLStatement, ExportStatement>(info->Copy());
}

} // namespace duckdb
