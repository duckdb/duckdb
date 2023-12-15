#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformRename(duckdb_libpgquery::PGRenameStmt &stmt) {
	if (!stmt.relation) {
		throw NotImplementedException("Altering schemas is not yet supported");
	}

	unique_ptr<AlterInfo> info;

	AlterEntryData data;
	data.if_not_found = TransformOnEntryNotFound(stmt.missing_ok);
	data.catalog = stmt.relation->catalogname ? stmt.relation->catalogname : INVALID_CATALOG;
	data.schema = stmt.relation->schemaname ? stmt.relation->schemaname : INVALID_SCHEMA;
	if (stmt.relation->relname) {
		data.name = stmt.relation->relname;
	}
	// first we check the type of ALTER
	switch (stmt.renameType) {
	case duckdb_libpgquery::PG_OBJECT_COLUMN: {
		// change column name

		// get the old name and the new name
		string old_name = stmt.subname;
		string new_name = stmt.newname;
		info = make_uniq<RenameColumnInfo>(std::move(data), old_name, new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_TABLE: {
		// change table name
		string new_name = stmt.newname;
		info = make_uniq<RenameTableInfo>(std::move(data), new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_VIEW: {
		// change view name
		string new_name = stmt.newname;
		info = make_uniq<RenameViewInfo>(std::move(data), new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_DATABASE:
	default:
		throw NotImplementedException("Schema element not supported yet!");
	}
	D_ASSERT(info);

	auto result = make_uniq<AlterStatement>();
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
