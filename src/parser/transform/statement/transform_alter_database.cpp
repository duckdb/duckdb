#include <string>
#include <utility>

#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/alter_database_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformAlterDatabase(duckdb_libpgquery::PGAlterDatabaseStmt &stmt) {
	auto result = make_uniq<AlterStatement>();

	auto database_name = stmt.dbname ? string(stmt.dbname) : string();
	if (database_name.empty()) {
		throw ParserException("ALTER DATABASE requires a database name");
	}

	OnEntryNotFound if_not_found = OnEntryNotFound::THROW_EXCEPTION;
	if (stmt.missing_ok) {
		if_not_found = OnEntryNotFound::RETURN_NULL;
	}

	switch (stmt.alter_type) {
	case duckdb_libpgquery::PG_ALTER_DATABASE_RENAME: {
		if (!stmt.new_name) {
			throw ParserException("ALTER DATABASE RENAME requires a new name");
		}
		auto info = make_uniq<RenameDatabaseInfo>(database_name, string(stmt.new_name), if_not_found);
		result->info = std::move(info);
		break;
	}
	default:
		throw ParserException("Unsupported ALTER DATABASE operation");
	}

	return result;
}

} // namespace duckdb
